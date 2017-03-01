﻿using System.Collections.Generic;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Models {
    public class NumberDictionary : Dictionary<string, long> {
        public NumberDictionary() {}

        public NumberDictionary(IDictionary<string, long> items) {
            this.AddRange(items);
        }
    }
}