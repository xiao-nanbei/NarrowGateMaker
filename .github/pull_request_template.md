## Summary

Describe the problem, the chosen change, and the observable result.

## Change Type

- [ ] Ordinary code bug fix
- [ ] Feature or mechanics change
- [ ] Tests or build-only change
- [ ] Public documentation change
- [ ] New execution attempt under an unchanged research identity
- [ ] New research identity because the frozen scientific contract changed
- [ ] Research evidence or receipt publication

## Validation

List the checks run and their results. Explain any check that could not run in a
public clone.

## Research and Evidence Identity

Complete this section when the change touches research or formal execution.

- Research identity:
- Identity classification: unchanged / changed / not applicable
- Changed contract elements: sample / baseline-candidates / folds / estimand / statistics / none
- Execution attempt ID:
- Exact clean public commit:
- Annotated execution tag:
- SHA-bound pre-run manifest:
- Final or failed receipt:
- Artifact availability:
- Development / Validation / sealed-holdout permissions:
- Prediction / action / deployment / live authority:

An implementation bug or performance repair must remain a new `attempt-*` under the
same research identity. It must not be presented as a new research `vXX`.

## Checklist

- [ ] I read [CONTRIBUTING.md](../CONTRIBUTING.md) and kept this PR focused.
- [ ] I added or updated tests appropriate to the behavioral risk.
- [ ] I used repository-relative links and approved placeholders.
- [ ] I included no personal path, private host, account state, credential, dataset,
      raw live record, or owner-side evidence.
- [ ] Every non-public artifact has honest availability; a hash is not presented as
      a downloadable location.
- [ ] I did not rewrite, move, or delete a frozen tag, manifest, or receipt.
- [ ] Presentation-only documentation changes are not described as reruns or new
      scientific results.
- [ ] Shadow output is not used as a substitute for full-path evidence or as action
      or live authority.
- [ ] I ran the relevant checks from [Developer Checks](../docs/dev/ci.md).
- [ ] Documentation changes pass the public documentation audit and
      `git diff --check`.

Research evidence changes must also follow the
[research evidence PR rules](../docs/opensource/research_contributions.md).
