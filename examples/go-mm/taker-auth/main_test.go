package main

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestSignTakerChallengeV1MatchesCrossLanguageVector(t *testing.T) {
	privateKey := strings.Repeat("11", 32)
	contractAddress := "inj19g43wyj843ydkc845dcdea6su4mgfjwnpjz6h5"
	takerAddress := "inj1r8n7xah8cgfm0el8u3kvwzja6zrd4le2krtp7d"
	nonce := "0x" + strings.Repeat("22", 32)
	const expiresAt = uint64(1772851186901)

	digest, err := takerChallengeDigest(contractAddress, takerAddress, nonce, expiresAt)
	if err != nil {
		t.Fatalf("takerChallengeDigest() error = %v", err)
	}
	const expectedDigest = "a6d1c5ab15e13e952ac30098f0b258a649e382c4b0a6cd505463c82b0793333c"
	if got := hex.EncodeToString(digest); got != expectedDigest {
		t.Fatalf("digest = %s, want %s", got, expectedDigest)
	}

	signature, err := signTakerChallengeV1(privateKey, contractAddress, takerAddress, nonce, expiresAt)
	if err != nil {
		t.Fatalf("signTakerChallengeV1() error = %v", err)
	}
	const expectedSignature = "0xee33449e2c3dbf11d1acdb11b2d24fdc976d287e5159391535889a55b89c34bb7f222b7c887a7a800344e62008f59b8264182e8b4b7bba556569854112e5bcab00"
	if signature != expectedSignature {
		t.Fatalf("signature = %s, want %s", signature, expectedSignature)
	}
}

func TestTakerChallengeDigestRejectsInvalidNonce(t *testing.T) {
	_, err := takerChallengeDigest(
		"inj19g43wyj843ydkc845dcdea6su4mgfjwnpjz6h5",
		"inj1r8n7xah8cgfm0el8u3kvwzja6zrd4le2krtp7d",
		"0x1234",
		1772851186901,
	)
	if err == nil {
		t.Fatal("takerChallengeDigest() accepted a nonce shorter than 32 bytes")
	}
}
