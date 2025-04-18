/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package options

// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
type ChecksumVerificationMode int

const (
	// NoVerification indicates DB should not verify checksum for SSTable blocks.
	NoVerification ChecksumVerificationMode = iota
	// OnTableRead indicates checksum should be verified while opening SSTtable.
	OnTableRead
	// OnBlockRead indicates checksum should be verified on every SSTable block read.
	OnBlockRead
	// OnTableAndBlockRead indicates checksum should be verified
	// on SSTable opening and on every block read.
	OnTableAndBlockRead
)

// CompressionType specifies how a block should be compressed.
type CompressionType uint32

const (
	// None mode indicates that a block is not compressed.
	// 无模式表示block未被压缩。
	None CompressionType = 0
	// Snappy mode indicates that a block is compressed using Snappy algorithm.
	// Snappy模式表示使用Snappy算法压缩block。
	Snappy CompressionType = 1
	// ZSTD mode indicates that a block is compressed using ZSTD algorithm.
	// ZSTD模式表示使用ZSTD算法压缩block。
	ZSTD CompressionType = 2
)
