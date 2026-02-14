package query

import (
	"testing"
)

func TestApplyOrderBy_SingleColumn(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "charlie", "age": int64(25)},
		{"name": "alice", "age": int64(30)},
		{"name": "bob", "age": int64(20)},
	}

	tests := []struct {
		name      string
		orderBy   []OrderByItem
		wantFirst string
		wantLast  string
	}{
		{
			name: "age ascending",
			orderBy: []OrderByItem{
				{Column: "age", Desc: false},
			},
			wantFirst: "bob",   // age 20
			wantLast:  "alice", // age 30
		},
		{
			name: "age descending",
			orderBy: []OrderByItem{
				{Column: "age", Desc: true},
			},
			wantFirst: "alice", // age 30
			wantLast:  "bob",   // age 20
		},
		{
			name: "name ascending",
			orderBy: []OrderByItem{
				{Column: "name", Desc: false},
			},
			wantFirst: "alice",
			wantLast:  "charlie",
		},
		{
			name: "name descending",
			orderBy: []OrderByItem{
				{Column: "name", Desc: true},
			},
			wantFirst: "charlie",
			wantLast:  "alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted, err := ApplyOrderBy(rows, tt.orderBy)
			if err != nil {
				t.Fatalf("ApplyOrderBy() error = %v", err)
			}

			if len(sorted) != len(rows) {
				t.Fatalf("ApplyOrderBy() returned %d rows, want %d", len(sorted), len(rows))
			}

			firstName := sorted[0]["name"].(string)
			if firstName != tt.wantFirst {
				t.Errorf("First row name = %s, want %s", firstName, tt.wantFirst)
			}

			lastName := sorted[len(sorted)-1]["name"].(string)
			if lastName != tt.wantLast {
				t.Errorf("Last row name = %s, want %s", lastName, tt.wantLast)
			}
		})
	}
}

func TestApplyOrderBy_MultipleColumns(t *testing.T) {
	rows := []map[string]interface{}{
		{"department": "sales", "name": "charlie", "age": int64(25)},
		{"department": "sales", "name": "alice", "age": int64(30)},
		{"department": "engineering", "name": "bob", "age": int64(20)},
		{"department": "engineering", "name": "david", "age": int64(35)},
	}

	orderBy := []OrderByItem{
		{Column: "department", Desc: false}, // engineering first, then sales
		{Column: "age", Desc: true},         // within department, oldest first
	}

	sorted, err := ApplyOrderBy(rows, orderBy)
	if err != nil {
		t.Fatalf("ApplyOrderBy() error = %v", err)
	}

	// Expected order:
	// 1. engineering, david (age 35)
	// 2. engineering, bob (age 20)
	// 3. sales, alice (age 30)
	// 4. sales, charlie (age 25)

	expected := []string{"david", "bob", "alice", "charlie"}
	for i, want := range expected {
		got := sorted[i]["name"].(string)
		if got != want {
			t.Errorf("Row %d name = %s, want %s", i, got, want)
		}
	}
}

func TestApplyOrderBy_NullValues(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "alice", "age": int64(30)},
		{"name": "bob", "age": nil},
		{"name": "charlie", "age": int64(25)},
	}

	orderBy := []OrderByItem{
		{Column: "age", Desc: false},
	}

	sorted, err := ApplyOrderBy(rows, orderBy)
	if err != nil {
		t.Fatalf("ApplyOrderBy() error = %v", err)
	}

	// NULL should sort first in ASC order
	firstName := sorted[0]["name"].(string)
	if firstName != "bob" {
		t.Errorf("First row (NULL) name = %s, want bob", firstName)
	}

	// Then 25, then 30
	if sorted[1]["name"].(string) != "charlie" {
		t.Errorf("Second row name = %s, want charlie", sorted[1]["name"].(string))
	}
	if sorted[2]["name"].(string) != "alice" {
		t.Errorf("Third row name = %s, want alice", sorted[2]["name"].(string))
	}
}

func TestApplyLimitOffset_Limit(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
		{"id": int64(4)},
		{"id": int64(5)},
	}

	tests := []struct {
		name      string
		limit     *int64
		offset    *int64
		wantCount int
		wantFirst int64
		wantLast  int64
	}{
		{
			name:      "limit 3",
			limit:     ptrInt64(3),
			offset:    nil,
			wantCount: 3,
			wantFirst: 1,
			wantLast:  3,
		},
		{
			name:      "limit 0",
			limit:     ptrInt64(0),
			offset:    nil,
			wantCount: 0,
			wantFirst: 0, // Not checked when wantCount is 0
			wantLast:  0, // Not checked when wantCount is 0
		},
		{
			name:      "limit exceeds rows",
			limit:     ptrInt64(10),
			offset:    nil,
			wantCount: 5,
			wantFirst: 1,
			wantLast:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyLimitOffset(rows, tt.limit, tt.offset)
			if err != nil {
				t.Fatalf("ApplyLimitOffset() error = %v", err)
			}

			if len(result) != tt.wantCount {
				t.Errorf("Result count = %d, want %d", len(result), tt.wantCount)
			}

			if len(result) > 0 {
				firstID := result[0]["id"].(int64)
				if firstID != tt.wantFirst {
					t.Errorf("First ID = %d, want %d", firstID, tt.wantFirst)
				}

				lastID := result[len(result)-1]["id"].(int64)
				if lastID != tt.wantLast {
					t.Errorf("Last ID = %d, want %d", lastID, tt.wantLast)
				}
			}
		})
	}
}

func TestApplyLimitOffset_Offset(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
		{"id": int64(4)},
		{"id": int64(5)},
	}

	tests := []struct {
		name      string
		limit     *int64
		offset    *int64
		wantCount int
		wantFirst int64
	}{
		{
			name:      "offset 2",
			limit:     nil,
			offset:    ptrInt64(2),
			wantCount: 3,
			wantFirst: 3,
		},
		{
			name:      "offset 0",
			limit:     nil,
			offset:    ptrInt64(0),
			wantCount: 5,
			wantFirst: 1,
		},
		{
			name:      "offset exceeds rows",
			limit:     nil,
			offset:    ptrInt64(10),
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyLimitOffset(rows, tt.limit, tt.offset)
			if err != nil {
				t.Fatalf("ApplyLimitOffset() error = %v", err)
			}

			if len(result) != tt.wantCount {
				t.Errorf("Result count = %d, want %d", len(result), tt.wantCount)
			}

			if len(result) > 0 {
				firstID := result[0]["id"].(int64)
				if firstID != tt.wantFirst {
					t.Errorf("First ID = %d, want %d", firstID, tt.wantFirst)
				}
			}
		})
	}
}

func TestApplyLimitOffset_Both(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
		{"id": int64(4)},
		{"id": int64(5)},
		{"id": int64(6)},
		{"id": int64(7)},
		{"id": int64(8)},
		{"id": int64(9)},
		{"id": int64(10)},
	}

	tests := []struct {
		name      string
		limit     *int64
		offset    *int64
		wantCount int
		wantFirst int64
		wantLast  int64
	}{
		{
			name:      "offset 3, limit 2",
			limit:     ptrInt64(2),
			offset:    ptrInt64(3),
			wantCount: 2,
			wantFirst: 4,
			wantLast:  5,
		},
		{
			name:      "offset 5, limit 10 (exceeds available)",
			limit:     ptrInt64(10),
			offset:    ptrInt64(5),
			wantCount: 5,
			wantFirst: 6,
			wantLast:  10,
		},
		{
			name:      "offset 0, limit 3",
			limit:     ptrInt64(3),
			offset:    ptrInt64(0),
			wantCount: 3,
			wantFirst: 1,
			wantLast:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyLimitOffset(rows, tt.limit, tt.offset)
			if err != nil {
				t.Fatalf("ApplyLimitOffset() error = %v", err)
			}

			if len(result) != tt.wantCount {
				t.Errorf("Result count = %d, want %d", len(result), tt.wantCount)
			}

			if len(result) > 0 {
				firstID := result[0]["id"].(int64)
				if firstID != tt.wantFirst {
					t.Errorf("First ID = %d, want %d", firstID, tt.wantFirst)
				}

				lastID := result[len(result)-1]["id"].(int64)
				if lastID != tt.wantLast {
					t.Errorf("Last ID = %d, want %d", lastID, tt.wantLast)
				}
			}
		})
	}
}
