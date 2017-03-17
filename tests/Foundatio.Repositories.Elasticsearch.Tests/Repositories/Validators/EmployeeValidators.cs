using System.Threading;
using System.Threading.Tasks;
using FluentValidation;
using FluentValidation.Results;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Validators {
    public class EmployeeValidatorWithRequiredFields : AbstractValidator<Employee> {
        public EmployeeValidatorWithRequiredFields() {
            RuleFor(o => o.Name).NotEmpty().WithMessage("Missing name");
            RuleFor(o => o.CompanyName).NotEmpty().WithMessage("Missing company name");
        }
    }

    public class EmployeeValidatorWithValidateException : AbstractValidator<Employee> {
        public override async Task<ValidationResult> ValidateAsync(ValidationContext<Employee> context, CancellationToken cancellation = default(CancellationToken)) {
            var result = await base.ValidateAsync(context, cancellation);

            result.Errors.Add(new ValidationFailure(nameof(EmployeeValidatorWithValidateException), "something went wrong here"));

            return result;
        }
    }
}
