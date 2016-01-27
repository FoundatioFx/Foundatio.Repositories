using System;
using System.Collections.Generic;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Extensions {
    public static class EnumerableExtensions {
        public static void EnsureIds<T>(this IEnumerable<T> values, Func<T, string> generateIdFunc) where T : class, IIdentity {
            if (values == null || generateIdFunc == null)
                return;

            foreach (var value in values) {
                if (value.Id == null)
                    value.Id = generateIdFunc(value);
            }
        }

        public static void SetDates<T>(this IEnumerable<T> values) where T : class, IHaveDates {
            if (values == null)
                return;

            foreach (var value in values) {
                if (value.CreatedUtc == DateTime.MinValue || value.CreatedUtc > DateTime.UtcNow)
                    value.CreatedUtc = DateTime.UtcNow;

                value.UpdatedUtc = DateTime.UtcNow;
            }
        }

        public static void SetCreatedDates<T>(this IEnumerable<T> values) where T : class, IHaveCreatedDate {
            if (values == null)
                return;

            foreach (var value in values) {
                if (value.CreatedUtc == DateTime.MinValue || value.CreatedUtc > DateTime.UtcNow)
                    value.CreatedUtc = DateTime.UtcNow;
            }
        }
    }
}
