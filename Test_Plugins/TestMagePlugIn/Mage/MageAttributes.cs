using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mage {

    public class MageAttributes : Attribute {

        public string ModType { get; set; }

        public string ModID { get; set; }

        public string ModLabel { get; set; }

        public string ModDescription { get; set; }

        public string ModClassName { get; set; }

        public MageAttributes(string type, string ID, string label, string description) {
            ModID = ID;
            ModLabel = label;
            ModType = type;
            ModDescription = description;
        }
    }
}
