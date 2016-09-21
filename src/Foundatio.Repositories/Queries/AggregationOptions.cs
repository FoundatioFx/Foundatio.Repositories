using System;
using System.Collections.Generic;
using System.Text;

namespace Foundatio.Repositories.Queries {
    internal class AggregationToken {
        public string String { get; set; }
        public string Nested { get; set; }

        public static List<AggregationToken> Tokenize(string input) {
            var tokens = new List<AggregationToken>();

            if (input.Trim().Length == 0)
                return tokens;

            var nestedLevel = 0;
            var token = new StringBuilder();
            var nestedToken = new StringBuilder();
            foreach (var c in input.ToCharArray()) {
                switch (c) {
                    case '(':
                        if (nestedLevel > 0)
                            nestedToken.Append(c);

                        nestedLevel += 1;
                        break;

                    case ')':
                        nestedLevel -= 1;

                        if (nestedLevel > 0)
                            nestedToken.Append(c);
                        break;

                    case ',':
                        if (nestedLevel > 0) {
                            nestedToken.Append(c);
                        } else {
                            tokens.Add(new AggregationToken {
                                String = token.ToString().Trim(),
                                Nested = nestedToken.ToString(),
                            });
                            token.Clear();
                            nestedToken.Clear();
                        }

                        break;

                    default:
                        if (nestedLevel > 0) {
                            nestedToken.Append(c);
                        } else {
                            token.Append(c);
                        }
                        break;
                }
            }

            tokens.Add(new AggregationToken {
                String = token.ToString().Trim(),
                Nested = nestedToken.ToString(),
            });

            return tokens;
        }
    }

    public class AggregationOptions {
        public static readonly AggregationOptions Empty = new AggregationOptions();

        public AggregationOptions() {
            Fields = new List<AggregationField>();
        }

        public List<AggregationField> Fields { get; }

        public static AggregationOptions Parse(string facets) {
            if (String.IsNullOrEmpty(facets))
                return AggregationOptions.Empty;

            var facetOptions = new AggregationOptions();
            var parsedFields = AggregationToken.Tokenize(facets);

            foreach (var field in parsedFields) {
                string name = field.String;
                int size = 25;
                var parts = field.String.Split(new[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length == 2) {
                    name = parts[0];
                    int partSize;
                    if (Int32.TryParse(parts[1], out partSize))
                        size = partSize;
                }

                facetOptions.Fields.Add(new AggregationField {
                    Field = name,
                    Size = size,
                    Nested = field.Nested.Length == 0 ? null : AggregationOptions.Parse(field.Nested),
                });
            }

            return facetOptions;
        }

        public static implicit operator AggregationOptions(string value) {
            return Parse(value);
        }
    }

    public class AggregationField {
        public string Field { get; set; }
        public int? Size { get; set; }

        public AggregationOptions Nested { get; set; }
    }
}