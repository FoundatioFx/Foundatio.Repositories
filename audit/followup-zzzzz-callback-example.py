"""Keep the documentation's progress example free of optional network side effects."""
import hashlib
from pathlib import Path

path = Path('docs/guide/jobs.md')
old = path.read_bytes()
assert hashlib.sha256(old).hexdigest() == '5c3e5fc92d759496368cbadc7f0207d96664992fbd048d874779ea4909b12ab8'
before = '''await configuration.ReindexAsync(async (progress, message) =>
{
    // progress: 0-100 percentage
    // message: Status description

    _logger.LogInformation("Reindex {Progress}%: {Message}", progress, message);

    // Update metrics or UI
    await UpdateProgressMetricAsync(progress);
});'''
after = '''await configuration.ReindexAsync((progress, message) =>
{
    _logger.LogInformation("Reindex {Progress}%: {Message}", progress, message);
    return Task.CompletedTask;
});'''
text = old.decode()
assert text.count(before) == 1
new = text.replace(before, after).encode()
assert hashlib.sha256(new).hexdigest() == '223c4891514389c9fa8aa3261f4c6f060f845e2d722c5c7bf6aec6569c0d5a7a'
path.write_bytes(new)
