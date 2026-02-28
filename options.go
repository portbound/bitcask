package bitcask

import (
	"time"
)

const (
	SyncStrategyUnset SyncStrategy = iota
	// SyncNone relies on the OS to flush data to disk. Highest performance, lowest durability.
	SyncNone

	// SyncAlways flushes to disk after every write operation. Highest durability, lower performance.
	SyncAlways
)

const (
	MergeStrategyUnset MergeStrategy = iota
	// MergeStrategyUnrestricted allows merging any time merge trigger thresholds are met.
	MergeStrategyUnrestricted

	// MergeStrategyNever disables automatic merging entirely.
	MergeStrategyNever

	// MergeStrategyWindow restricts merging to a specific hour range (0 - 23).
	MergeStrategyWindow
)

// SyncStrategy defines how data is flushed to disk to ensure durability.
type SyncStrategy int

// MergeStrategy defines the strategy for reclaiming space from stale data files.
type MergeStrategy int

// MergePolicy holds the configuration for the chosen merge policy.
type MergePolicy struct {
	Strategy          MergeStrategy
	Interval          time.Duration // How often merge thresholds are evaluated.
	WindowStart       int           // Hour (0-23)
	WindowEnd         int           // Hour (0-23)
	FragThreshold     uint8         // Fragmentation percentage (0-100) to trigger a merge.
	DeadByteThreshold uint64        // Minimum dead bytes required to trigger a merge.
}

type bitcaskOpts struct {
	Dir          string
	MaxFileSize  uint64
	MergePolicy  MergePolicy
	SyncStrategy SyncStrategy
}

var defaultOpts = bitcaskOpts{
	Dir:         ".",
	MaxFileSize: uint64(128 * 1024 * 1024), // 128MB
	MergePolicy: MergePolicy{
		Strategy:          MergeStrategyUnrestricted,
		Interval:          3 * time.Minute,
		WindowStart:       0,
		WindowEnd:         0,
		FragThreshold:     60,
		DeadByteThreshold: uint64(512 * 1024 * 1024), // 512MB
	},
	SyncStrategy: SyncAlways,
}

// Option is a functional option for configuring a Bitcask instance.
type Option func(*Bitcask)

// WithWorkDir sets the base directory where Bitcask will store its metadata and logs.
// The default value is ".".
func WithWorkDir(dir string) Option {
	return func(b *Bitcask) {
		b.opts.Dir = dir
	}
}

// WithMaxFileSize sets the maximum size a data file can reach before it is rotated.
// The default value is 128MB.
func WithMaxFileSize(size uint64) Option {
	return func(b *Bitcask) {
		b.opts.MaxFileSize = size
	}
}

// WithMergePolicy configures the strategy and thresholds for reclaiming disk space.
func WithMergePolicy(config MergePolicy) Option {
	return func(b *Bitcask) {
		b.opts.MergePolicy = config
	}
}

// WithSyncStrategy defines how frequently data is flushed to disk.
// Using SyncAlways provides the highest durability but impacts write performance.
// The default value is SyncAlways.
func WithSyncStrategy(strategy SyncStrategy) Option {
	return func(b *Bitcask) {
		b.opts.SyncStrategy = strategy
	}
}
