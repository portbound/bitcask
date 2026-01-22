package main

import (
	"testing"
)

func TestBitcask_Put(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		path string
		// Named input parameters for target function.
		k       []byte
		v       []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBitcask(tt.path)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			gotErr := b.Put(tt.k, tt.v)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Put() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Put() succeeded unexpectedly")
			}
		})
	}
}


func Test_encodeRecord(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		k    []byte
		v    []byte
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeRecord(tt.k, tt.v)
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("encodeRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

