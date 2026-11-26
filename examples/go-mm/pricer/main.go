/*
 * RFQ pricer (native gRPC): read indicative RFQ prices without sending an RFQ.
 *
 * Follows the pricing stream of each market.
 * Public and read-only: no keys, no auth.
 *
 * Env:
 *   PRICER_GRPC_ENDPOINT  default rfq-pricer.grpc.injective.network:443 (mainnet)
 *   PRICER_MARKET_IDS     comma-separated market IDs, default mainnet BTC/USDC PERP
 */
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "mm-scripts-go/proto/injective_rfq_pricer_rpc"
)

const (
	defaultEndpoint  = "rfq-pricer.grpc.injective.network:443"
	defaultMarketIDs = "0x0ee7ca44147bab6ec81ac293b5fe7915488e612af59964b2d663d6008d861dee"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func orDash(price string) string {
	if price == "" {
		return "-"
	}
	return price
}

func show(marketID string, ts int64, markPrice string, tiers []*pb.RFQPricingTierType) {
	if len(tiers) == 0 {
		fmt.Printf("%s ts=%d withdrawn (no current pricing)\n", marketID, ts)
		return
	}
	fmt.Printf("%s ts=%d mark=%s\n", marketID, ts, markPrice)
	for _, tier := range tiers {
		fmt.Printf("  $%d: bid=%s ask=%s age=%dms\n",
			tier.NotionalUsd, orDash(tier.Bid), orDash(tier.Ask), tier.AgeMs)
	}
}

func main() {
	_ = godotenv.Load()
	endpoint := envOr("PRICER_GRPC_ENDPOINT", defaultEndpoint)
	var marketIDs []string
	for _, id := range strings.Split(envOr("PRICER_MARKET_IDS", defaultMarketIDs), ",") {
		if id = strings.TrimSpace(id); id != "" {
			marketIDs = append(marketIDs, id)
		}
	}

	creds := credentials.NewTLS(nil)
	if strings.HasPrefix(endpoint, "localhost") || strings.HasPrefix(endpoint, "127.0.0.1") {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("connect %s: %v", endpoint, err)
	}
	defer conn.Close()
	client := pb.NewInjectiveRfqPricerRPCClient(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Println("streaming, Ctrl+C to stop")
	stream, err := client.PricingStream(ctx, &pb.PricingStreamRequest{MarketIds: marketIDs})
	if err != nil {
		log.Fatalf("PricingStream: %v", err)
	}
	for {
		res, err := stream.Recv()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Fatalf("PricingStream recv: %v", err)
		}
		show(res.MarketId, res.Ts, res.MarkPrice, res.Tiers)
	}
}
