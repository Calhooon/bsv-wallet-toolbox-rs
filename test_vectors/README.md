# Test Vectors for rust-wallet-toolbox

This directory contains test vectors extracted from the TypeScript and Go implementations of wallet-toolbox, enabling cross-implementation compatibility testing.

## Directory Structure

```
test_vectors/
├── README.md                    # This file
├── storage/
│   ├── create_action/           # CreateAction request/response test vectors
│   ├── list_outputs/            # ListOutputs request/response test vectors
│   └── list_actions/            # ListActions request/response test vectors
├── transactions/                # Transaction-related test vectors (merkle paths, BEEF, etc.)
└── keys/                        # Key derivation and BRC29 test vectors
```

## Source Implementations

- **TypeScript**: `/Users/johncalhoun/bsv/wallet-toolbox/test/`
- **Go**: `/Users/johncalhoun/bsv/go-wallet-toolbox/pkg/`

## Test Vector Format

All test vectors are stored as JSON files with the following conventions:

### Common Fields

- `description`: Human-readable description of the test case
- `expected_error`: If present, the test expects an error matching this pattern
- `inputs`: Input parameters for the operation
- `expected_outputs`: Expected results (if not an error case)

### Validation Test Vectors

Test vectors for argument validation follow this pattern:

```json
{
  "description": "Test case description",
  "inputs": {
    // Operation-specific input fields
  },
  "expected_error": "error pattern or null for success cases",
  "expected_outputs": {
    // Expected results for success cases
  }
}
```

## Usage in Rust Tests

```rust
use serde_json;
use std::fs;

#[derive(Deserialize)]
struct TestVector {
    description: String,
    inputs: serde_json::Value,
    expected_error: Option<String>,
    expected_outputs: Option<serde_json::Value>,
}

#[test]
fn test_from_vectors() {
    let data = fs::read_to_string("test_vectors/storage/create_action/validation.json")
        .expect("Failed to read test vectors");
    let vectors: Vec<TestVector> = serde_json::from_str(&data)
        .expect("Failed to parse test vectors");

    for vector in vectors {
        // Run test against each vector
    }
}
```

## Constants and Reference Values

Common test values used across implementations:

### Test Users (from Go testusers)

| User  | Private Key (hex)                                          |
|-------|-----------------------------------------------------------|
| Alice | `143ab18a84d3b25e1a13cefa90038411e5d2014590a2a4a57263d1593c8dee1c` |
| Bob   | `0881208859876fc227d71bfb8b91814462c5164b6fee27e614798f6e85d2547d` |

### Storage Configuration (from Go fixtures)

| Constant                | Value                                                              |
|------------------------|---------------------------------------------------------------------|
| StorageServerPrivKey    | `8143f5ed6c5b41c3d084d39d49e161d8dde4b50b0685a4e4ac23959d3b8a319b` |
| StorageIdentityKey      | `028f2daab7808b79368d99eef1ebc2d35cdafe3932cafe3d83cf17837af034ec29` |
| UserIdentityKeyHex      | `03f17660f611ce531402a2ce1e070380b6fde57aca211d707bfab27bce42d86beb` |
| DerivationPrefix        | `Pg==` (Base64)                                                     |
| DerivationSuffix        | `Sg==` (Base64)                                                     |

### BRC29 Test Keys (from Go brc29 fixtures)

| Key                    | Value                                                              |
|-----------------------|--------------------------------------------------------------------|
| senderPrivateKeyHex   | `143ab18a84d3b25e1a13cefa90038411e5d2014590a2a4a57263d1593c8dee1c` |
| senderPublicKeyHex    | `0320bbfb879bbd6761ecd2962badbb41ba9d60ca88327d78b07ae7141af6b6c810` |
| senderWIF             | `Kwu2vS6fqkd5WnRgB9VXd4vYpL9mwkXePZWtG9Nr5s6JmfHcLsQr`             |
| recipientPrivateKeyHex| `0000000000000000000000000000000000000000000000000000000000000001` |
| recipientPublicKeyHex | `0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798` |
| expectedAddress       | `19bxE1pRYYtjZeQm7P8e2Ws5zMkm8NNuxx`                               |
| expectedTestnetAddress| `mp7uX4uQMaKzLktNpx71rS5QrMMTzDP12u`                               |

### Pagination Limits (from Go validate)

| Constant              | Value   |
|----------------------|---------|
| MaxPaginationLimit    | 10000   |
| MaxPaginationOffset   | (varies)|
| DefaultLimit          | 100     |
| DefaultOffset         | 0       |

### Maximum Values

| Constant              | Value           |
|----------------------|-----------------|
| MaxSatoshis          | 2100000000000000 |
| MaxDescriptionLength | 2000            |
| MinDescriptionLength | 5               |
| MaxLabelLength       | 300             |
| MaxBasketLength      | 300             |
| MaxTagLength         | 300             |

## Cross-Implementation Notes

### TypeScript Specific
- Uses Jest for testing
- Test database: `walletLegacyTestData.sqlite`
- Key generation via `_tu.getKeyPair()`

### Go Specific
- Uses testify/require for assertions
- Fixtures in `pkg/internal/fixtures/`
- Test users in `pkg/internal/fixtures/testusers/`

### Differences to Note
- TypeScript uses camelCase, Go uses PascalCase for struct fields
- Go uses pointers for optional fields (`*bool`), TypeScript uses `undefined`
- Both implementations should produce identical transaction IDs and signatures for the same inputs
