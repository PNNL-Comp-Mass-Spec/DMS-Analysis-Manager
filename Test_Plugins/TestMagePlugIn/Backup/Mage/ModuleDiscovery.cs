using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.IO;

// supports discovery and dynamic loading of Mage pipeline modules
// with special support for filters and their associated parameter panels
namespace Mage {

    public class ModuleDiscovery {

        #region Member Variables

        #endregion

        #region Properties

        public static string ExternalModuleFolder { get; set; }

        #endregion

        #region General Discovery Functions

        public static Type GetModuleTypeFromClassName(string ClassName) {
            Type modType = null;
            if (modType == null) {
                // is the module class in the executing assembly?
                Assembly ae = Assembly.GetExecutingAssembly(); //GetType().Assembly;
                //modType = ae.GetType(ClassName); // should work, but doesn't
                //string ne = ae.GetName().Name;
                //modType = Type.GetType(ne + "." + ClassName); // does work, but do it the long way for consistency
                modType = GetClassTypeFromAssembly(ClassName, ae);
            }
            if (modType == null) {
                // is the module class in the main assembly?
                Assembly aa = Assembly.GetEntryAssembly();
                modType = GetClassTypeFromAssembly(ClassName, aa);
            }
            if (modType == null) {
                // is the module class in the assembly of the code that called us?
                Assembly ac = Assembly.GetCallingAssembly(); //GetType().Assembly;
                //string nc = ac.GetName().Name;
                // modType = Type.GetType(nc + "." + ClassName); // should work, but doesn't
                modType = GetClassTypeFromAssembly(ClassName, ac);
            }
            if (modType == null) {
                // is the module class found in a loadable assembly?
                DirectoryInfo di = new DirectoryInfo(ExternalModuleFolder);
                FileInfo[] dllFiles = di.GetFiles("Loadable*.dll");
                foreach (FileInfo fi in dllFiles) {
                    string DLLName = fi.Name;
                    string path = Path.Combine(ExternalModuleFolder, DLLName);
                    Assembly af = Assembly.LoadFrom(path);
                    modType = GetClassTypeFromAssembly(ClassName, af);
                    if (modType != null) break; // we found it, don't keep looking
                } // foreach
            }
            return modType;
        }

        // we should have been able to use assembly.GetType("className"), 
        // but it doesn't seem to work.  This is the work-around
        private static Type GetClassTypeFromAssembly(string ClassName, Assembly assembly) {
            Type modType = null;
            Type[] ts = assembly.GetTypes();
            foreach (Type t in ts) {
                if (t.Name == ClassName) {
                    modType = t;
                    break;
                }
            }
            return modType;
        }

        #endregion

        #region Filter Discovery support

        // list of attributes for filters and parameter panels
        private static List<MageAttributes> mFilterList = new List<MageAttributes>();

        // list of attributes for filters, and filter panels, indexed by ID
        private static Dictionary<string, MageAttributes> mFilters = new Dictionary<string, MageAttributes>();
        private static Dictionary<string, MageAttributes> mPanels = new Dictionary<string, MageAttributes>();

        // list of attributes for filters, indexed by label
        private static Dictionary<string, MageAttributes> mFiltersByLabel = new Dictionary<string, MageAttributes>();


        // get list of filter labels (for display)
        public static string[] FilterLabels {
            get {
                List<string> labels = new List<string>();
                foreach (MageAttributes ma in mFilters.Values) {
                    labels.Add(ma.ModLabel);
                }
                return labels.ToArray();
            }
        }

        // return class name for selected filter
        public static string SelectedFilterClassName(string filterLabel) {
            string sel = "";
            if (mFiltersByLabel.ContainsKey(filterLabel)) {
                sel = mFiltersByLabel[filterLabel].ModClassName;
            }
            return sel;
        }

        // find name of parameter panel associated with currently selected filter, if there is one
        public static string GetParameterPanelForFilter(string filterLabel) {
            string panelClass = "";
            if (mFiltersByLabel.ContainsKey(filterLabel)) {
                string ID = mFiltersByLabel[filterLabel].ModID;
                if (mPanels.ContainsKey(ID)) {
                    panelClass = mPanels[ID].ModClassName;
                }
            }
            return panelClass;
        }

        // discover filter modules and their associated parameter panels
        // and set up the necessary internal properties, components, and variables
        public static void SetupFilters() {
            mFilterList.Clear();
            mFilterList = ModuleDiscovery.FindFilters();
            foreach (MageAttributes ma in mFilterList) {
                if (ma.ModType == "Filter") {
                    mFilters.Add(ma.ModID, ma);
                    mFiltersByLabel.Add(ma.ModLabel, ma);
                } else if (ma.ModType == "FilterPanel") {
                    mPanels.Add(ma.ModID, ma);
                }
            }
        }

        // find filter modules in main assembly and loadable assemblies
        // and add to master list
        public static List<MageAttributes> FindFilters() {
            // list to hold info about discovered filters
            List<MageAttributes> filterList = new List<MageAttributes>();

            // list to hold classes that we will look at
            List<Type> classesToExamine = new List<Type>();

            // add classes from main assembly
            classesToExamine.AddRange(Assembly.GetEntryAssembly().GetTypes());

            // get classes from loadable DLLs
            DirectoryInfo di = new DirectoryInfo(ExternalModuleFolder);
            List<FileInfo> dllFiles = new List<FileInfo>();
            dllFiles.AddRange(di.GetFiles("Loadable*.dll"));
            foreach (FileInfo fi in dllFiles) {
                string DLLName = fi.Name;
                string path = Path.Combine(ExternalModuleFolder, DLLName);
                classesToExamine.AddRange(Assembly.LoadFrom(path).GetTypes());
            }

            // look at each class in list to see if it is marked with
            // Mage attributes and examine them to find filter modules
            foreach (Type modType in classesToExamine) {
                Console.WriteLine(modType.ToString());
                object[] atrbs = modType.GetCustomAttributes(false);
                foreach (object obj in atrbs) {
                    if (obj.GetType() == typeof(MageAttributes)) {
                        MageAttributes ma = (MageAttributes)obj;
                        ma.ModClassName = modType.Name;
                        filterList.Add(ma);
                    }
                }
            }
            return filterList;
        }

        #endregion

    }
}
