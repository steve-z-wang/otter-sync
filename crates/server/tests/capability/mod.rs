//! Current transport envelopes for existing server behavior fixtures.
pub fn request(bytes: &[u8]) -> Vec<u8> {
    match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(value) if value.is_object() => {
            axton_core::with_capabilities(bytes, &[axton_core::SCOPE_MEMBERSHIP_CAPABILITY])
                .unwrap()
        }
        _ => bytes.to_vec(),
    }
}

#[allow(dead_code)]
pub fn pull(bytes: &[u8]) -> axton_core::Result<axton_core::PullPage> {
    let page = axton_core::ScopePullPage::decode(bytes)?;
    let mut records = std::collections::BTreeMap::new();
    for change in page.changes {
        if let axton_core::ScopeChange::Upsert { record, .. } = change {
            records.insert(
                (
                    record.model.clone(),
                    serde_json::to_string(&record.identity)?,
                ),
                record,
            );
        }
    }
    Ok(axton_core::PullPage {
        cursors: page.cursors,
        changes: records.into_values().collect(),
    })
}
#[allow(dead_code)]
pub fn bootstrap(bytes: &[u8]) -> axton_core::Result<axton_core::BootstrapPage> {
    let page = axton_core::ScopeBootstrapPage::decode(bytes)?;
    let mut records: Vec<_> = page
        .changes
        .into_iter()
        .filter_map(|change| match change {
            axton_core::ScopeChange::Upsert { record, .. } => Some(record),
            _ => None,
        })
        .collect();
    records.sort_by_key(|r| (r.model.clone(), serde_json::to_string(&r.identity).unwrap()));
    Ok(axton_core::BootstrapPage {
        scope: page.scope,
        from: page.from,
        to: page.to,
        until: page.until,
        head: page.head,
        records,
    })
}
