//! Unit tests of `lookup.rs`, kept in their own file.

use super::*;

#[test]
fn test_overlay_certificate_serde() {
    let cert = OverlayCertificate {
        type_id: "dGVzdC10eXBl".to_string(),
        serial_number: "abc123".to_string(),
        subject: "02aabbccdd".to_string(),
        certifier: "03eeff0011".to_string(),
        revocation_outpoint: Some("deadbeef.0".to_string()),
        fields: {
            let mut m = HashMap::new();
            m.insert("name".to_string(), "encrypted_value".to_string());
            m
        },
        keyring: HashMap::new(),
        signature: Some("3045022100...".to_string()),
        decrypted_fields: HashMap::new(),
    };

    let json = serde_json::to_string(&cert).unwrap();
    let deserialized: OverlayCertificate = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.type_id, cert.type_id);
    assert_eq!(deserialized.serial_number, cert.serial_number);
    assert_eq!(deserialized.subject, cert.subject);
    assert_eq!(deserialized.certifier, cert.certifier);
    assert_eq!(deserialized.revocation_outpoint, cert.revocation_outpoint);
    assert_eq!(deserialized.fields.len(), 1);
    assert_eq!(deserialized.signature, cert.signature);
}

#[test]
fn test_http_resolver_creation() {
    let resolver = HttpLookupResolver::new("https://example.com");
    assert_eq!(resolver.slap_trackers.len(), 1);
    assert_eq!(resolver.slap_trackers[0], "https://example.com");

    let resolver_multi =
        HttpLookupResolver::with_endpoints(vec!["https://a.com".into(), "https://b.com".into()]);
    assert_eq!(resolver_multi.slap_trackers.len(), 2);
}

#[test]
fn test_http_resolver_mainnet() {
    let resolver = HttpLookupResolver::mainnet();
    assert!(!resolver.slap_trackers.is_empty());
    assert!(resolver.slap_trackers[0].contains("bsvb.tech"));
}

#[test]
fn test_overlay_certificate_to_identity_certificate() {
    let cert = OverlayCertificate {
        type_id: "dGVzdC10eXBl".to_string(),
        serial_number: "abc123".to_string(),
        subject: "02aabbccdd".to_string(),
        certifier: "03eeff0011".to_string(),
        revocation_outpoint: Some("deadbeef.0".to_string()),
        fields: {
            let mut m = HashMap::new();
            m.insert("name".to_string(), "encrypted_value".to_string());
            m
        },
        keyring: {
            let mut m = HashMap::new();
            m.insert("name".to_string(), "keyring_value".to_string());
            m
        },
        signature: Some("3045022100...".to_string()),
        decrypted_fields: {
            let mut m = HashMap::new();
            m.insert("name".to_string(), "Alice".to_string());
            m
        },
    };

    let identity_cert = cert.to_identity_certificate();
    assert_eq!(identity_cert.certificate.certificate_type, "dGVzdC10eXBl");
    assert_eq!(identity_cert.certificate.serial_number, "abc123");
    assert_eq!(identity_cert.certificate.subject, "02aabbccdd");
    assert_eq!(identity_cert.certificate.certifier, "03eeff0011");
    assert_eq!(identity_cert.certificate.revocation_outpoint, "deadbeef.0");
    assert_eq!(identity_cert.certificate.signature, "3045022100...");
    assert!(identity_cert.publicly_revealed_keyring.is_some());
    assert!(identity_cert.decrypted_fields.is_some());
    assert_eq!(
        identity_cert.decrypted_fields.unwrap().get("name").unwrap(),
        "Alice"
    );
}

#[test]
fn test_overlay_certificate_dedup() {
    // Create two certificates with same (type_id, serial_number)
    let cert1 = IdentityCertificate {
        certificate: WalletCertificate {
            certificate_type: "type1".to_string(),
            serial_number: "sn1".to_string(),
            subject: "subject1".to_string(),
            certifier: "certifier1".to_string(),
            revocation_outpoint: String::new(),
            fields: HashMap::new(),
            signature: String::new(),
        },
        certifier_info: None,
        publicly_revealed_keyring: None,
        decrypted_fields: None,
    };

    let cert2 = IdentityCertificate {
        certificate: WalletCertificate {
            certificate_type: "type1".to_string(),
            serial_number: "sn1".to_string(),
            subject: "subject1".to_string(),
            certifier: "certifier1".to_string(),
            revocation_outpoint: String::new(),
            fields: HashMap::new(),
            signature: "different_sig".to_string(),
        },
        certifier_info: None,
        publicly_revealed_keyring: None,
        decrypted_fields: None,
    };

    // Different (type_id, serial_number) should be kept
    let cert3 = IdentityCertificate {
        certificate: WalletCertificate {
            certificate_type: "type2".to_string(),
            serial_number: "sn2".to_string(),
            subject: "subject2".to_string(),
            certifier: "certifier2".to_string(),
            revocation_outpoint: String::new(),
            fields: HashMap::new(),
            signature: String::new(),
        },
        certifier_info: None,
        publicly_revealed_keyring: None,
        decrypted_fields: None,
    };

    let certs = vec![cert1, cert2, cert3];
    let deduped = dedup_certificates(certs);
    assert_eq!(deduped.len(), 2);
    assert_eq!(deduped[0].certificate.certificate_type, "type1");
    assert_eq!(deduped[1].certificate.certificate_type, "type2");
}

#[test]
fn test_overlay_certificate_fields() {
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), "encrypted_name".to_string());
    fields.insert("email".to_string(), "encrypted_email".to_string());
    fields.insert("phone".to_string(), "encrypted_phone".to_string());

    let cert = OverlayCertificate {
        type_id: "dGVzdC10eXBl".to_string(),
        serial_number: "serial-001".to_string(),
        subject: "02aabbccdd".to_string(),
        certifier: "03eeff0011".to_string(),
        revocation_outpoint: Some("txid.0".to_string()),
        fields: fields.clone(),
        keyring: HashMap::new(),
        signature: Some("sig".to_string()),
        decrypted_fields: HashMap::new(),
    };

    assert_eq!(cert.fields.len(), 3);
    assert_eq!(cert.fields.get("name").unwrap(), "encrypted_name");
    assert_eq!(cert.fields.get("email").unwrap(), "encrypted_email");
    assert_eq!(cert.fields.get("phone").unwrap(), "encrypted_phone");
    assert_eq!(
        cert.dedup_key(),
        ("dGVzdC10eXBl".to_string(), "serial-001".to_string())
    );
}

#[test]
fn test_overlay_certificate_empty_optional_fields() {
    let cert = OverlayCertificate {
        type_id: "type".to_string(),
        serial_number: "sn".to_string(),
        subject: "subject".to_string(),
        certifier: "certifier".to_string(),
        revocation_outpoint: None,
        fields: HashMap::new(),
        keyring: HashMap::new(),
        signature: None,
        decrypted_fields: HashMap::new(),
    };

    let identity = cert.to_identity_certificate();
    // revocation_outpoint and signature become empty strings (not Optional in WalletCertificate)
    assert_eq!(identity.certificate.revocation_outpoint, "");
    assert_eq!(identity.certificate.signature, "");
    assert!(identity.publicly_revealed_keyring.is_none());
    assert!(identity.decrypted_fields.is_none());
}

#[test]
fn test_parse_overlay_answer_empty_output_list() {
    let answer = LookupAnswer::OutputList { outputs: vec![] };
    let certs = parse_overlay_answer(&answer).unwrap();
    assert!(certs.is_empty());
}

#[test]
fn test_parse_overlay_answer_freeform() {
    let answer = LookupAnswer::Freeform {
        result: serde_json::json!({"some": "data"}),
    };
    let certs = parse_overlay_answer(&answer).unwrap();
    assert!(certs.is_empty());
}

#[test]
fn test_multiple_certificates_same_identity() {
    // Same subject but different certificate types
    let cert1 = OverlayCertificate {
        type_id: "social_cert".to_string(),
        serial_number: "sn1".to_string(),
        subject: "02aabbccdd".to_string(),
        certifier: "03certifier1".to_string(),
        revocation_outpoint: None,
        fields: {
            let mut m = HashMap::new();
            m.insert("name".to_string(), "Alice".to_string());
            m
        },
        keyring: HashMap::new(),
        signature: Some("sig1".to_string()),
        decrypted_fields: HashMap::new(),
    };

    let cert2 = OverlayCertificate {
        type_id: "email_cert".to_string(),
        serial_number: "sn2".to_string(),
        subject: "02aabbccdd".to_string(),
        certifier: "03certifier2".to_string(),
        revocation_outpoint: None,
        fields: {
            let mut m = HashMap::new();
            m.insert("email".to_string(), "alice@example.com".to_string());
            m
        },
        keyring: HashMap::new(),
        signature: Some("sig2".to_string()),
        decrypted_fields: HashMap::new(),
    };

    let certs = [cert1, cert2];
    let identity_certs: Vec<IdentityCertificate> =
        certs.iter().map(|c| c.to_identity_certificate()).collect();

    assert_eq!(identity_certs.len(), 2);
    assert_eq!(
        identity_certs[0].certificate.certificate_type,
        "social_cert"
    );
    assert_eq!(identity_certs[1].certificate.certificate_type, "email_cert");
    // Same subject on both
    assert_eq!(identity_certs[0].certificate.subject, "02aabbccdd");
    assert_eq!(identity_certs[1].certificate.subject, "02aabbccdd");

    // Dedup should keep both since they have different (type_id, serial_number)
    let deduped = dedup_certificates(identity_certs);
    assert_eq!(deduped.len(), 2);
}
