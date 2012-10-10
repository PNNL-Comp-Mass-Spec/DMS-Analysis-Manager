using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mage {

    public interface IModuleParameters {

        Dictionary<string, string> GetParameters();

        void SetParameters(Dictionary<string, string> paramList);

    }
}
