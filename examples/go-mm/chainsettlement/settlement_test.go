package chainsettlement

import "testing"

func stringPtr(v string) *string {
	return &v
}

func TestMakerHasTraded(t *testing.T) {
	const maker = "inj1maker"

	tests := []struct {
		name    string
		results []rfqQuoteResult
		want    bool
	}{
		{
			name: "non-zero quantity",
			results: []rfqQuoteResult{
				{Maker: maker, Quantity: stringPtr("1.25")},
			},
			want: true,
		},
		{
			name: "non-zero margin",
			results: []rfqQuoteResult{
				{Maker: maker, Margin: stringPtr("10")},
			},
			want: true,
		},
		{
			name: "non-zero quantity and margin",
			results: []rfqQuoteResult{
				{Maker: maker, Quantity: stringPtr("2"), Margin: stringPtr("20")},
			},
			want: true,
		},
		{
			name: "zero quantity and margin",
			results: []rfqQuoteResult{
				{Maker: maker, Quantity: stringPtr("0"), Margin: stringPtr("0")},
			},
			want: false,
		},
		{
			name: "nil quantity and margin",
			results: []rfqQuoteResult{
				{Maker: maker},
			},
			want: false,
		},
		{
			name: "different maker traded",
			results: []rfqQuoteResult{
				{Maker: "inj1other", Quantity: stringPtr("1"), Margin: stringPtr("10")},
			},
			want: false,
		},
		{
			name: "error only",
			results: []rfqQuoteResult{
				{Maker: maker, Error: stringPtr("insufficient margin")},
			},
			want: false,
		},
		{
			name: "invalid quantity and margin",
			results: []rfqQuoteResult{
				{Maker: maker, Quantity: stringPtr("not-a-number"), Margin: stringPtr("also-invalid")},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settlement := &rfqSettlement{QuoteResults: tt.results}
			if got := makerHasTraded(settlement, maker); got != tt.want {
				t.Fatalf("makerHasTraded() = %v, want %v", got, tt.want)
			}
		})
	}
}
