//! #419 item 2: an overlay certificate keeps its field and keyring order.

use bsv_rs::wallet::IdentityCertifier;
use bsv_wallet_toolbox_rs::wallet::lookup::OverlayCertificate;

const NAMES: [&str; 8] = [
    "zeta", "alpha", "mike", "bravo", "yankee", "charlie", "xray", "delta",
];

#[test]
fn overlay_certificate_maps_keep_the_received_order_at_the_wallet_boundary() {
    let entries = NAMES
        .iter()
        .map(|name| format!("\"{name}\":\"value\""))
        .collect::<Vec<_>>()
        .join(",");
    let text = format!(
        "{{\"type\":\"type\",\"serialNumber\":\"serial\",\"subject\":\"subject\",\
         \"certifier\":\"certifier\",\"fields\":{{{entries}}},\
         \"keyring\":{{{entries}}},\"decryptedFields\":{{{entries}}}}}"
    );
    let overlay: OverlayCertificate = serde_json::from_str(&text).unwrap();
    let result = overlay.to_identity_certificate(IdentityCertifier {
        name: "fixture".into(),
        icon_url: String::new(),
        description: String::new(),
        trust: 1,
    });
    for names in [
        overlay.fields.keys().collect::<Vec<_>>(),
        overlay.keyring.keys().collect(),
        overlay.decrypted_fields.keys().collect(),
        result.certificate.fields.keys().collect(),
        result.publicly_revealed_keyring.keys().collect(),
        result.decrypted_fields.keys().collect(),
    ] {
        assert_eq!(names, NAMES);
    }
    let encoded = serde_json::to_string(&overlay).unwrap();
    for member in ["fields", "keyring", "decryptedFields"] {
        assert!(encoded.contains(&format!("\"{member}\":{{{entries}}}")));
    }
}
