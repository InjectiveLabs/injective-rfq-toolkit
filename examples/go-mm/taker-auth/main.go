/*
 * TakerStream authentication probe (native gRPC).
 *
 * This example only performs the optional v1 authentication handshake. It
 * does not create an RFQ or submit an on-chain transaction.
 */
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "mm-scripts-go/proto/injective_rfq_rpc"
)

const (
	domainType              = "EIP712Domain(string name,string version,address verifyingContract)"
	takerChallengeType      = "TakerStreamAuthChallenge(address taker,bytes32 nonce,uint64 expiresAt)"
	domainName              = "RFQ"
	domainVersion           = "1"
	defaultHandshakeTimeout = 15 * time.Second
)

func trim0x(value string) string {
	return strings.TrimPrefix(strings.TrimPrefix(value, "0x"), "0X")
}

func bech32ToEVM(address string) ([20]byte, error) {
	hrp, raw, err := bech32.DecodeAndConvert(address)
	if err != nil {
		return [20]byte{}, fmt.Errorf("decode bech32 address %q: %w", address, err)
	}
	if hrp != "inj" {
		return [20]byte{}, fmt.Errorf("expected inj address, got hrp %q", hrp)
	}
	if len(raw) != 20 {
		return [20]byte{}, fmt.Errorf("expected a 20-byte address, got %d bytes", len(raw))
	}

	var result [20]byte
	copy(result[:], raw)
	return result, nil
}

func encodeAddress(address [20]byte) []byte {
	word := make([]byte, 32)
	copy(word[12:], address[:])
	return word
}

func encodeUint64(value uint64) []byte {
	word := make([]byte, 32)
	binary.BigEndian.PutUint64(word[24:], value)
	return word
}

func takerChallengeDigest(contractAddress, takerAddress, nonce string, expiresAt uint64) ([]byte, error) {
	contract, err := bech32ToEVM(contractAddress)
	if err != nil {
		return nil, fmt.Errorf("contract address: %w", err)
	}
	taker, err := bech32ToEVM(takerAddress)
	if err != nil {
		return nil, fmt.Errorf("taker address: %w", err)
	}
	nonceBytes, err := hex.DecodeString(trim0x(nonce))
	if err != nil {
		return nil, fmt.Errorf("decode challenge nonce: %w", err)
	}
	if len(nonceBytes) != 32 {
		return nil, fmt.Errorf("challenge nonce must contain 32 bytes, got %d", len(nonceBytes))
	}

	domainPayload := make([]byte, 0, 32*4)
	domainPayload = append(domainPayload, ethcrypto.Keccak256([]byte(domainType))...)
	domainPayload = append(domainPayload, ethcrypto.Keccak256([]byte(domainName))...)
	domainPayload = append(domainPayload, ethcrypto.Keccak256([]byte(domainVersion))...)
	domainPayload = append(domainPayload, encodeAddress(contract)...)
	domainSeparator := ethcrypto.Keccak256(domainPayload)

	messagePayload := make([]byte, 0, 32*4)
	messagePayload = append(messagePayload, ethcrypto.Keccak256([]byte(takerChallengeType))...)
	messagePayload = append(messagePayload, encodeAddress(taker)...)
	messagePayload = append(messagePayload, nonceBytes...)
	messagePayload = append(messagePayload, encodeUint64(expiresAt)...)
	messageHash := ethcrypto.Keccak256(messagePayload)

	digestPayload := make([]byte, 0, 66)
	digestPayload = append(digestPayload, 0x19, 0x01)
	digestPayload = append(digestPayload, domainSeparator...)
	digestPayload = append(digestPayload, messageHash...)
	return ethcrypto.Keccak256(digestPayload), nil
}

func signTakerChallengeV1(privateKey, contractAddress, takerAddress, nonce string, expiresAt uint64) (string, error) {
	digest, err := takerChallengeDigest(contractAddress, takerAddress, nonce, expiresAt)
	if err != nil {
		return "", err
	}
	key, err := ethcrypto.HexToECDSA(trim0x(privateKey))
	if err != nil {
		return "", fmt.Errorf("decode private key: %w", err)
	}
	signature, err := ethcrypto.Sign(digest, key)
	if err != nil {
		return "", fmt.Errorf("sign challenge: %w", err)
	}
	return hexutil.Encode(signature), nil
}

func signerAddress(privateKey string) (string, error) {
	key, err := ethcrypto.HexToECDSA(trim0x(privateKey))
	if err != nil {
		return "", fmt.Errorf("decode private key: %w", err)
	}
	return bech32.ConvertAndEncode("inj", ethcrypto.PubkeyToAddress(key.PublicKey).Bytes())
}

func isLoopbackTarget(target string) bool {
	host := target
	if parts := strings.SplitN(target, "://", 2); len(parts) == 2 {
		host = parts[1]
	}
	host = strings.TrimPrefix(host, "dns:///")
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func privateKeyFromEnvironment() (privateKey string, ephemeral bool, err error) {
	privateKey = os.Getenv("RETAIL_PRIVATE_KEY")
	if privateKey == "" {
		privateKey = os.Getenv("TESTNET_RETAIL_PRIVATE_KEY")
	}
	if privateKey != "" {
		return privateKey, false, nil
	}

	key, err := ethcrypto.GenerateKey()
	if err != nil {
		return "", false, fmt.Errorf("generate ephemeral private key: %w", err)
	}
	return hexutil.Encode(ethcrypto.FromECDSA(key)), true, nil
}

func run() error {
	_ = godotenv.Load(".env", "../.env", "../../.env")

	grpcEndpoint := os.Getenv("GRPC_ENDPOINT")
	if grpcEndpoint == "" {
		grpcEndpoint = os.Getenv("RFQ_GRPC_URL")
	}
	if grpcEndpoint == "" {
		return fmt.Errorf("GRPC_ENDPOINT (or RFQ_GRPC_URL) is not set")
	}
	contractAddress := os.Getenv("CONTRACT_ADDRESS")
	if contractAddress == "" {
		contractAddress = os.Getenv("RFQ_CONTRACT_ADDRESS")
	}
	if contractAddress == "" {
		return fmt.Errorf("CONTRACT_ADDRESS (or RFQ_CONTRACT_ADDRESS) is not set")
	}

	privateKey, ephemeral, err := privateKeyFromEnvironment()
	if err != nil {
		return err
	}
	derivedAddress, err := signerAddress(privateKey)
	if err != nil {
		return err
	}
	takerAddress := os.Getenv("TAKER_REQUEST_ADDRESS")
	if takerAddress == "" {
		takerAddress = derivedAddress
	}
	if _, err := bech32ToEVM(takerAddress); err != nil {
		return fmt.Errorf("TAKER_REQUEST_ADDRESS: %w", err)
	}
	if _, err := bech32ToEVM(contractAddress); err != nil {
		return fmt.Errorf("CONTRACT_ADDRESS: %w", err)
	}

	if ephemeral {
		fmt.Println("No retail key configured; using an ephemeral signer for this handshake.")
	}
	fmt.Printf("Connecting to TakerStream at %s\n", grpcEndpoint)
	fmt.Printf("Request address: %s\n", takerAddress)
	if takerAddress != derivedAddress {
		fmt.Printf("Signer address:  %s (must have an Authz grant)\n", derivedAddress)
	}

	var transportCredentials credentials.TransportCredentials
	if isLoopbackTarget(grpcEndpoint) {
		transportCredentials = insecure.NewCredentials()
	} else {
		transportCredentials = credentials.NewTLS(nil)
	}
	connection, err := grpc.NewClient(grpcEndpoint, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return fmt.Errorf("connect to gRPC: %w", err)
	}
	defer connection.Close()

	baseContext, cancel := context.WithTimeout(context.Background(), defaultHandshakeTimeout)
	defer cancel()
	streamContext := metadata.NewOutgoingContext(baseContext, metadata.Pairs(
		"request_address", takerAddress,
		"auth_version", "v1",
	))
	stream, err := pb.NewInjectiveRfqRPCClient(connection).TakerStream(streamContext)
	if err != nil {
		return fmt.Errorf("open TakerStream: %w", err)
	}
	defer stream.CloseSend()

	// This starts the bidirectional stream without creating an RFQ.
	if err := stream.Send(&pb.TakerStreamStreamingRequest{MessageType: "ping"}); err != nil {
		return fmt.Errorf("send initial ping: %w", err)
	}

	var challengeNonce string
	for {
		response, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive TakerStream response: %w", err)
		}

		switch response.GetMessageType() {
		case "pong":
			continue
		case "challenge":
			challenge := response.GetChallenge()
			if challenge == nil {
				return fmt.Errorf("challenge response did not contain a challenge")
			}
			if challenge.GetExpiresAt() < 0 {
				return fmt.Errorf("challenge expires_at must not be negative")
			}
			signature, err := signTakerChallengeV1(
				privateKey,
				contractAddress,
				takerAddress,
				challenge.GetNonce(),
				uint64(challenge.GetExpiresAt()),
			)
			if err != nil {
				return fmt.Errorf("process authentication challenge: %w", err)
			}
			if err := stream.Send(&pb.TakerStreamStreamingRequest{
				MessageType: "auth",
				Auth:        &pb.TakerAuth{Signature: signature},
			}); err != nil {
				return fmt.Errorf("send authentication response: %w", err)
			}
			challengeNonce = challenge.GetNonce()
			fmt.Println("Challenge signed and sent.")
		case "auth_result":
			result := response.GetAuthResult()
			if result == nil {
				return fmt.Errorf("auth_result response did not contain a result")
			}
			if challengeNonce == "" {
				return fmt.Errorf("received auth_result before a challenge")
			}
			if !strings.EqualFold(result.GetNonce(), challengeNonce) {
				return fmt.Errorf("auth_result nonce does not match the challenge")
			}
			fmt.Printf(
				"Authentication result: authenticated=%t code=%s nonce=%s message=%s\n",
				result.GetAuthenticated(),
				result.GetCode(),
				result.GetNonce(),
				result.GetMessage_(),
			)
			if !result.GetAuthenticated() {
				return fmt.Errorf("taker authentication failed with code %q", result.GetCode())
			}
			return nil
		case "error":
			streamError := response.GetError()
			if streamError == nil {
				return fmt.Errorf("TakerStream returned an empty error response")
			}
			return fmt.Errorf("TakerStream error %q: %s", streamError.GetCode(), streamError.GetMessage_())
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "taker auth probe:", err)
		os.Exit(1)
	}
}
