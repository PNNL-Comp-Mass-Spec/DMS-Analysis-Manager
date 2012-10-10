using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;

namespace LoadableModuleExample {

    [MageAttributes("Filter", "Fake", "Fake filter", "Another example of loadable filter")]
    public class FakeFilter : ContentFilter {

        protected override bool CheckFilter(ref object[] vals) {
            return true;
        }

    }

}
