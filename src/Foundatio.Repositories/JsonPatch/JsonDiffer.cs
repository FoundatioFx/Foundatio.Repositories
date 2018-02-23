using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.JsonPatch {
    public class JsonDiffer {
        internal static string Extend(string path, string extension) {
            // TODO: JSON property name needs escaping for path ??
            return path + "/" + extension;
        }

        private static Operation Build(string op, string path, string key, JToken value) {
            if (String.IsNullOrEmpty(key))
                return Operation.Parse("{ 'op' : '" + op + "' , path: '" + path + "', value: " +
                                    (value == null ? "null" : value.ToString(Formatting.None)) + "}");

            return Operation.Parse("{ op : '" + op + "' , path : '" + Extend(path, key) + "' , value : " +
                                (value == null ? "null" : value.ToString(Formatting.None)) + "}");
        }

        internal static Operation Add(string path, string key, JToken value) {
            return Build("add", path, key, value);
        }

        internal static Operation Remove(string path, string key) {
            return Build("remove", path, key, null);
        }

        internal static Operation Replace(string path, string key, JToken value) {
            return Build("replace", path, key, value);
        }

        internal static IEnumerable<Operation> CalculatePatch(JToken left, JToken right, bool useIdToDetermineEquality,
            string path = "") {
            if (left.Type != right.Type) {
                yield return JsonDiffer.Replace(path, "", right);
                yield break;
            }

            if (left.Type == JTokenType.Array) {
                Operation prev = null;
                foreach (var operation in ProcessArray(left, right, path, useIdToDetermineEquality)) {
                    var add = operation as AddOperation;
                    if (prev is RemoveOperation prevRemove && add != null && add.Path == prevRemove.Path) {
                        yield return Replace(add.Path, "", add.Value);
                        prev = null;
                    } else {
                        if (prev != null)
                            yield return prev;
                        prev = operation;
                    }
                }

                if (prev != null)
                    yield return prev;
            } else if (left.Type == JTokenType.Object) {
                var lprops = ((IDictionary<string, JToken>)left).OrderBy(p => p.Key);
                var rprops = ((IDictionary<string, JToken>)right).OrderBy(p => p.Key);

                foreach (var removed in lprops.Except(rprops, MatchesKey.Instance)) {
                    yield return JsonDiffer.Remove(path, removed.Key);
                }

                foreach (var added in rprops.Except(lprops, MatchesKey.Instance)) {
                    yield return JsonDiffer.Add(path, added.Key, added.Value);
                }

                var matchedKeys = lprops.Select(x => x.Key).Intersect(rprops.Select(y => y.Key));
                var zipped = matchedKeys.Select(k => new { key = k, left = left[k], right = right[k] });

                foreach (var match in zipped) {
                    string newPath = path + "/" + match.key;
                    foreach (var patch in CalculatePatch(match.left, match.right, useIdToDetermineEquality, newPath))
                        yield return patch;
                }
                yield break;
            } else {
                // Two values, same type, not JObject so no properties

                if (left.ToString() == right.ToString())
                    yield break;
                else
                    yield return JsonDiffer.Replace(path, "", right);
            }
        }

        private static IEnumerable<Operation> ProcessArray(JToken left, JToken right, string path,
            bool useIdPropertyToDetermineEquality) {
            var comparer = new CustomCheckEqualityComparer(useIdPropertyToDetermineEquality, new JTokenEqualityComparer());

            int commonHead = 0;
            int commonTail = 0;
            var array1 = left.ToArray();
            int len1 = array1.Length;
            var array2 = right.ToArray();
            int len2 = array2.Length;
            //    if (len1 == 0 && len2 ==0 ) yield break;
            while (commonHead < len1 && commonHead < len2) {
                if (comparer.Equals(array1[commonHead], array2[commonHead]) == false)
                    break;

                //diff and yield objects here
                foreach (var operation in CalculatePatch(array1[commonHead], array2[commonHead], useIdPropertyToDetermineEquality, path + "/" + commonHead)) {
                    yield return operation;
                }
                commonHead++;

            }

            // separate common tail
            while (commonTail + commonHead < len1 && commonTail + commonHead < len2) {
                if (comparer.Equals(array1[len1 - 1 - commonTail], array2[len2 - 1 - commonTail]) == false)
                    break;

                int index1 = len1 - 1 - commonTail;
                int index2 = len2 - 1 - commonTail;
                foreach (var operation in CalculatePatch(array1[index1], array2[index2], useIdPropertyToDetermineEquality, path + "/" + index1)) {
                    yield return operation;
                }
                commonTail++;
            }

            if (commonHead == 0 && commonTail == 0 && len2 > 0 && len1 > 0) {
                yield return new ReplaceOperation {
                    Path = path,
                    Value = new JArray(array2)
                };
                yield break;
            }


            var leftMiddle = array1.Skip(commonHead).Take(array1.Length - commonTail - commonHead).ToArray();
            var rightMiddle = array2.Skip(commonHead).Take(array2.Length - commonTail - commonHead).ToArray();
            foreach (var jToken in leftMiddle) {
                yield return new RemoveOperation {
                    Path = path + "/" + commonHead
                };
            }
            for (int i = 0; i < rightMiddle.Length; i++) {
                yield return new AddOperation {
                    Value = rightMiddle[i],
                    Path = path + "/" + (i + commonHead)
                };
            }
        }

        private class MatchesKey : IEqualityComparer<KeyValuePair<string, JToken>> {
            public static MatchesKey Instance = new MatchesKey();

            public bool Equals(KeyValuePair<string, JToken> x, KeyValuePair<string, JToken> y) {
                return x.Key.Equals(y.Key);
            }

            public int GetHashCode(KeyValuePair<string, JToken> obj) {
                return obj.Key.GetHashCode();
            }
        }

        public PatchDocument Diff(JToken @from, JToken to, bool useIdPropertyToDetermineEquality) {
            return new PatchDocument(CalculatePatch(@from, to, useIdPropertyToDetermineEquality).ToArray());
        }
    }

    internal class CustomCheckEqualityComparer : IEqualityComparer<JToken> {
        private readonly bool _enableIdCheck;
        private readonly IEqualityComparer<JToken> _inner;

        public CustomCheckEqualityComparer(bool enableIdCheck, IEqualityComparer<JToken> inner) {
            _enableIdCheck = enableIdCheck;
            _inner = inner;
        }

        public bool Equals(JToken x, JToken y) {
            if (!_enableIdCheck || x.Type != JTokenType.Object || y.Type != JTokenType.Object)
                return _inner.Equals(x, y);

            var xIdToken = x["id"];
            var yIdToken = y["id"];

            string xId = xIdToken?.Value<string>();
            string yId = yIdToken?.Value<string>();
            if (xId != null && xId == yId) {
                return true;
            }

            return _inner.Equals(x, y);
        }

        public int GetHashCode(JToken obj) {
            if (!_enableIdCheck || obj.Type != JTokenType.Object)
                return _inner.GetHashCode(obj);

            var xIdToken = obj["id"];
            string xId = xIdToken != null && xIdToken.HasValues ? xIdToken.Value<string>() : null;
            if (xId != null)
                return xId.GetHashCode() + _inner.GetHashCode(obj);

            return _inner.GetHashCode(obj);
        }

        public static bool HaveEqualIds(JToken x, JToken y) {
            if (x.Type != JTokenType.Object || y.Type != JTokenType.Object)
                return false;

            var xIdToken = x["id"];
            var yIdToken = y["id"];

            string xId = xIdToken?.Value<string>();
            string yId = yIdToken?.Value<string>();

            return xId != null && xId == yId;
        }
    }
}