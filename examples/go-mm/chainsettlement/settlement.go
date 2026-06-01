package chainsettlement

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	pb "mm-scripts-go/proto/injective_rfq_rpc"
)

// StreamMakerSettlements subscribes to chain settlement events and emits
// settlement updates only when the connected maker has a quote in the event.
func StreamMakerSettlements(
	ctx context.Context,
	endpoint string,
	contractAddr string,
	makerAddr string,
	out chan<- *pb.RFQSettlementMakerUpdate,
) error {
	client, err := rpchttp.New(endpoint)
	if err != nil {
		return fmt.Errorf("cometBFT client: %w", err)
	}
	if err := client.Start(); err != nil {
		return fmt.Errorf("cometBFT start: %w", err)
	}
	defer client.Stop()

	query := fmt.Sprintf("tm.event='Tx' AND wasm-rfq-accept-quote._contract_address='%s'", contractAddr)
	fmt.Println("   Query:   ", query)

	events, err := client.Subscribe(ctx, "go-mm-rfq-settlements", query, 100)
	if err != nil {
		return fmt.Errorf("cometBFT subscribe: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-events:
			if !ok {
				return fmt.Errorf("cometBFT subscription closed")
			}
			settlement, err := settlementFromCometEvents(event.Events)
			if err != nil {
				log.Printf("settlement event decode error: %v", err)
				continue
			}
			if settlement == nil || !makerHasTraded(settlement, makerAddr) {
				continue
			}
			out <- convertSettlementToMakerUpdate(settlement, event.Events)
		}
	}
}

func firstEventValue(events map[string][]string, key string) string {
	values := events[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func prefixedEventAttrs(events map[string][]string, eventType string) map[string]string {
	prefix := eventType + "."
	attrs := make(map[string]string)
	for key, values := range events {
		if !strings.HasPrefix(key, prefix) || len(values) == 0 {
			continue
		}
		attrs[strings.TrimPrefix(key, prefix)] = values[0]
	}
	return attrs
}

func parseUintAttr(attrs map[string]string, key string) uint64 {
	if attrs[key] == "" {
		return 0
	}
	v, err := strconv.ParseUint(attrs[key], 10, 64)
	if err != nil {
		log.Printf("invalid %s on settlement event: %v", key, err)
		return 0
	}
	return v
}

func parseUintPtrAttr(attrs map[string]string, key string) *uint64 {
	if attrs[key] == "" {
		return nil
	}
	v, err := strconv.ParseUint(attrs[key], 10, 64)
	if err != nil {
		log.Printf("invalid %s on settlement event: %v", key, err)
		return nil
	}
	return &v
}

func parseUint32PtrAttr(attrs map[string]string, key string) *uint32 {
	if attrs[key] == "" {
		return nil
	}
	v, err := strconv.ParseUint(attrs[key], 10, 32)
	if err != nil {
		log.Printf("invalid %s on settlement event: %v", key, err)
		return nil
	}
	out := uint32(v)
	return &out
}

func unmarshalJSONAttr[T any](attrs map[string]string, key string, out *T) {
	raw := attrs[key]
	if raw == "" {
		return
	}
	if err := json.Unmarshal([]byte(raw), out); err != nil {
		log.Printf("invalid JSON %s on settlement event: %v", key, err)
	}
}

func settlementFromCometEvents(events map[string][]string) (*rfqSettlement, error) {
	const eventType = "wasm-rfq-accept-quote"

	for _, key := range []string{
		eventType + ".settlement",
		eventType + ".data",
		eventType + ".payload",
	} {
		if raw := firstEventValue(events, key); raw != "" {
			var settlement rfqSettlement
			if err := json.Unmarshal([]byte(raw), &settlement); err != nil {
				return nil, fmt.Errorf("unmarshal %s: %w", key, err)
			}
			return &settlement, nil
		}
	}

	attrs := prefixedEventAttrs(events, eventType)
	if len(attrs) == 0 {
		return nil, nil
	}

	settlement := &rfqSettlement{
		ContractAddress:         attrs["_contract_address"],
		RfqID:                   parseUintAttr(attrs, "rfq_id"),
		MarketID:                attrs["market_id"],
		Taker:                   attrs["taker"],
		ExecutionMode:           attrs["execution_mode"],
		Relayer:                 stringPtrAttr(attrs, "relayer"),
		Epoch:                   parseUintPtrAttr(attrs, "epoch"),
		SubaccountNonce:         parseUint32PtrAttr(attrs, "subaccount_nonce"),
		LaneVersion:             parseUintPtrAttr(attrs, "lane_version"),
		Direction:               attrs["direction"],
		Margin:                  attrs["margin"],
		Quantity:                attrs["quantity"],
		WorstPrice:              attrs["worst_price"],
		FallbackQuantity:        attrs["fallback_quantity"],
		FallbackMargin:          attrs["fallback_margin"],
		FallbackOrderQuantity:   attrs["fallback_order_quantity"],
		FallbackDroppedQuantity: attrs["fallback_dropped_quantity"],
		Cid:                     attrs["cid"],
	}
	unmarshalJSONAttr(attrs, "quotes", &settlement.Quotes)
	unmarshalJSONAttr(attrs, "results", &settlement.QuoteResults)
	unmarshalJSONAttr(attrs, "unfilled_action", &settlement.UnfilledAction)
	return settlement, nil
}

func stringPtrAttr(attrs map[string]string, key string) *string {
	if attrs[key] == "" {
		return nil
	}
	v := attrs[key]
	return &v
}

func makerHasTraded(settlement *rfqSettlement, maker string) bool {
	for _, quote := range settlement.QuoteResults {
		if quote.Maker != maker {
			continue
		}
		var (
			qty = decimal.Zero
			mgn = decimal.Zero
			err error
		)
		if quote.Quantity != nil {
			qty, err = decimal.NewFromString(*quote.Quantity)
			if err != nil {
				fmt.Printf("error parsing quote quantity %s: %v", *quote.Quantity, err)
				qty = decimal.Zero
			}
		}
		if quote.Margin != nil {
			mgn, err = decimal.NewFromString(*quote.Margin)
			if err != nil {
				fmt.Printf("error parsing quote margin %s: %v", *quote.Margin, err)
				mgn = decimal.Zero
			}
		}
		return !qty.IsZero() || !mgn.IsZero()
	}
	return false
}

func settlementQuoteStatus(result rfqQuoteResult) string {
	if result.Error != nil && *result.Error != "" {
		return "rejected"
	}
	if result.Quantity != nil || result.Margin != nil {
		return "accepted"
	}
	return ""
}

func convertSettlementQuote(quote rfqSettlementQuote, result rfqQuoteResult) *pb.RFQSettlementQuote {
	out := &pb.RFQSettlementQuote{
		Maker:            quote.Maker,
		Price:            quote.Price,
		QuotedMargin:     quote.Margin,
		QuotedQuantity:   quote.Quantity,
		ExecutedMargin:   ptrStringValue(result.Margin),
		ExecutedQuantity: ptrStringValue(result.Quantity),
		Signature:        quote.Signature,
		Status:           settlementQuoteStatus(result),
	}
	if quote.Expiry != nil {
		out.Expiry = &pb.RFQExpiryType{
			Timestamp: quote.Expiry.Ts,
			Height:    quote.Expiry.Height,
		}
	}
	if quote.Nonce != nil {
		out.Nonce = *quote.Nonce
	}
	return out
}

func ptrStringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func convertUnfilledAction(action *rfqUnfilledAction) *pb.RFQSettlementUnfilledActionType {
	if action == nil {
		return nil
	}
	out := &pb.RFQSettlementUnfilledActionType{}
	if action.Limit != nil {
		out.Limit = &pb.RFQSettlementLimitActionType{Price: action.Limit.Price}
	}
	if action.Market != nil {
		out.Market = &pb.RFQSettlementMarketActionType{}
	}
	return out
}

func convertSettlementToMakerUpdate(settlement *rfqSettlement, events map[string][]string) *pb.RFQSettlementMakerUpdate {
	resultsByMaker := make(map[string]rfqQuoteResult)
	for _, quote := range settlement.QuoteResults {
		resultsByMaker[quote.Maker] = quote
	}
	quotes := make([]*pb.RFQSettlementQuote, 0, len(settlement.Quotes))
	for _, quote := range settlement.Quotes {
		quotes = append(quotes, convertSettlementQuote(quote, resultsByMaker[quote.Maker]))
	}

	return &pb.RFQSettlementMakerUpdate{
		Quotes:           quotes,
		RfqId:            settlement.RfqID,
		MarketId:         settlement.MarketID,
		Taker:            settlement.Taker,
		Direction:        settlement.Direction,
		Margin:           settlement.Margin,
		Quantity:         settlement.Quantity,
		WorstPrice:       settlement.WorstPrice,
		UnfilledAction:   convertUnfilledAction(settlement.UnfilledAction),
		FallbackQuantity: settlement.FallbackQuantity,
		FallbackMargin:   settlement.FallbackMargin,
		Height:           parseUintAttrFromEvents(events, "tx.height"),
		Cid:              settlement.Cid,
		TxHash:           firstEventValue(events, "tx.hash"),
	}
}

func parseUintAttrFromEvents(events map[string][]string, key string) uint64 {
	raw := firstEventValue(events, key)
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		log.Printf("invalid %s on settlement event: %v", key, err)
		return 0
	}
	return v
}

type rfqExpiry struct {
	Ts     uint64 `json:"ts,omitempty"`
	Height uint64 `json:"h,omitempty"`
}

type rfqSettlementQuote struct {
	Maker     string     `json:"maker"`
	Margin    string     `json:"margin"`
	Price     string     `json:"price"`
	Quantity  string     `json:"quantity"`
	Expiry    *rfqExpiry `json:"expiry,omitempty"`
	Signature string     `json:"signature"`
	Nonce     *uint64    `json:"nonce,omitempty"`
}

type rfqUnfilledAction struct {
	Limit  *rfqLimitAction  `json:"limit,omitempty"`
	Market *rfqMarketAction `json:"market,omitempty"`
}

type rfqLimitAction struct {
	Price string `json:"price"`
}

type rfqMarketAction struct{}

type rfqQuoteResult struct {
	Maker    string  `json:"maker"`
	Error    *string `json:"e,omitempty"`
	Quantity *string `json:"q,omitempty"`
	Margin   *string `json:"m,omitempty"`
}

type rfqSettlement struct {
	ContractAddress         string               `json:"_contract_address"`
	RfqID                   uint64               `json:"rfq_id"`
	MarketID                string               `json:"market_id"`
	Taker                   string               `json:"taker"`
	ExecutionMode           string               `json:"execution_mode"`
	Relayer                 *string              `json:"relayer,omitempty"`
	Epoch                   *uint64              `json:"epoch,omitempty"`
	SubaccountNonce         *uint32              `json:"subaccount_nonce,omitempty"`
	LaneVersion             *uint64              `json:"lane_version,omitempty"`
	Direction               string               `json:"direction"`
	Margin                  string               `json:"margin"`
	Quantity                string               `json:"quantity"`
	WorstPrice              string               `json:"worst_price"`
	Quotes                  []rfqSettlementQuote `json:"quotes"`
	QuoteResults            []rfqQuoteResult     `json:"results"`
	UnfilledAction          *rfqUnfilledAction   `json:"unfilled_action,omitempty"`
	FallbackQuantity        string               `json:"fallback_quantity"`
	FallbackMargin          string               `json:"fallback_margin"`
	FallbackOrderQuantity   string               `json:"fallback_order_quantity"`
	FallbackDroppedQuantity string               `json:"fallback_dropped_quantity"`
	Cid                     string               `json:"cid"`
}
