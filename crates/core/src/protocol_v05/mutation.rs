//! Canonical batch hashing, receipt membership and slot reconstruction.
use super::*;

fn batch_body(request: &MutationRequest) -> Result<Value> {
    request.context.validate()?;
    positive(request.batch_id)?;
    if request.mutations.is_empty() {
        return Err(invalid("empty Mutation Batch"));
    }
    let mut previous = 0;
    for mutation in &request.mutations {
        positive(mutation.id)?;
        if mutation.id <= previous {
            return Err(invalid("Mutation IDs must increase"));
        }
        previous = mutation.id;
        text(&mutation.name)?;
        positive(mutation.version)?;
        text(&mutation.descriptor)?;
        reconstruct_input(&mutation.operations)?;
    }
    let mut value = serde_json::to_value(request)?;
    value.as_object_mut().unwrap().remove("digest");
    Ok(value)
}
/// Hashes canonical ordered intent, excluding only the digest field itself.
pub fn batch_digest(request: &MutationRequest) -> Result<String> {
    hash("axton:mutation-batch:5", &batch_body(request)?)
}
pub fn validate_batch(request: &MutationRequest) -> Result<()> {
    digest(&request.digest)?;
    if batch_digest(request)? != request.digest {
        return Err(invalid("Batch digest mismatch"));
    }
    Ok(())
}
pub fn validate_acknowledgement(
    request: &MutationRequest,
    ack: &BatchAcknowledgement,
) -> Result<()> {
    validate_batch(request)?;
    ack.validate()?;
    if ack.context != request.context
        || ack.batch_id != request.batch_id
        || ack.digest != request.digest
    {
        return Err(invalid("Batch acknowledgement mismatch"));
    }
    let expected: BTreeSet<_> = request.mutations.iter().map(|m| m.id).collect();
    let actual: BTreeSet<_> = ack.results.iter().map(|m| m.mutation_id).collect();
    if expected != actual {
        return Err(invalid("Batch acknowledgement membership mismatch"));
    }
    for mutation in &request.mutations {
        if let MutationOutcome::Accepted { targets, .. } = &ack
            .results
            .iter()
            .find(|r| r.mutation_id == mutation.id)
            .unwrap()
            .outcome
        {
            let supplied: BTreeSet<_> = targets
                .iter()
                .map(|t| t.key().encoded())
                .collect::<Result<_>>()?;
            for op in &mutation.operations {
                if let Some(model) = &op.model
                    && !supplied.contains(
                        &RecordKey {
                            model: model.clone(),
                            identity: op.identity.clone(),
                        }
                        .encoded()?,
                    )
                {
                    return Err(invalid("missing mandatory settlement target"));
                }
            }
        }
    }
    Ok(())
}
/// Rebuild generated slot input without a second persisted input blob.
/// Paths are `slot` or `slot[index]`; retained descriptor normalization remains
/// required before handler execution (use normalize_action_args).
pub fn reconstruct_input(operations: &[MutationOperation]) -> Result<Value> {
    let mut slots = BTreeMap::<String, Value>::new();
    let mut lists = BTreeMap::<String, BTreeMap<usize, Value>>::new();
    let mut previous = None;
    for op in operations {
        op.validate()?;
        if previous.is_some_and(|p| op.step <= p) {
            return Err(invalid("steps must increase"));
        }
        previous = Some(op.step);
        let (slot, index) = if let Some((slot, tail)) = op.input_path.split_once('[') {
            let digits = tail
                .strip_suffix(']')
                .ok_or_else(|| invalid("invalid input path"))?;
            let index = digits
                .parse::<usize>()
                .map_err(|_| invalid("invalid list index"))?;
            if index.to_string() != digits {
                return Err(invalid("noncanonical list index"));
            }
            (slot, Some(index))
        } else {
            (op.input_path.as_str(), None)
        };
        if slot.is_empty()
            || !slot
                .chars()
                .enumerate()
                .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))
        {
            return Err(invalid("invalid slot name"));
        }
        let value = if op.operation == Operation::Argument {
            op.value.clone()
        } else {
            let mut full = op.identity.as_object().unwrap().clone();
            for (k, v) in op.value.as_object().into_iter().flatten() {
                if full.insert(k.clone(), v.clone()).is_some() {
                    return Err(invalid("payload repeats identity"));
                }
            }
            Value::Object(full)
        };
        if let Some(index) = index {
            if slots.contains_key(slot)
                || lists
                    .entry(slot.into())
                    .or_default()
                    .insert(index, value)
                    .is_some()
            {
                return Err(invalid("overlapping input path"));
            }
        } else if lists.contains_key(slot) || slots.insert(slot.into(), value).is_some() {
            return Err(invalid("overlapping input path"));
        }
    }
    for (slot, list) in lists {
        if list.keys().copied().ne(0..list.len()) {
            return Err(invalid("noncontiguous list input"));
        }
        slots.insert(slot, Value::Array(list.into_values().collect()));
    }
    Ok(Value::Object(slots.into_iter().collect()))
}
/// Identity of the complete retained Mutation descriptor, including snapshots.
pub fn mutation_descriptor_digest(descriptor: &crate::ActionDescriptor) -> Result<String> {
    hash(
        "axton:mutation-descriptor:5",
        &serde_json::to_value(descriptor)?,
    )
}
/// Canonical read descriptor context under the protocol-5 domain.
pub fn materialization_id(schema: &crate::Schema, projection_generation: &str) -> Result<String> {
    materialization_id_for(
        schema,
        &schema
            .models
            .iter()
            .map(|m| (m.name.clone(), m.version))
            .collect(),
        projection_generation,
    )
}
/// Retained versions use their exact read descriptor and reachable enum values.
/// The caller supplies retained descriptors in Schema.result_models.
pub fn materialization_id_for(
    schema: &crate::Schema,
    models: &BTreeMap<String, u64>,
    projection_generation: &str,
) -> Result<String> {
    let bytes = crate::canonical_json(&crate::materialization::contract(
        schema,
        models,
        projection_generation,
    )?)?;
    let mut digest = Sha256::new();
    digest.update(b"axton:materialization:5\0");
    digest.update(bytes.as_bytes());
    Ok(format!("{:x}", digest.finalize()))
}
