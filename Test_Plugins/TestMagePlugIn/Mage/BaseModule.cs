using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using log4net;

// this class provides basic functions that are of use to most pipeline module classes.

namespace Mage {

    public class BaseModule : IBaseModule {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        public static string ModuleFunction = "";
        public static string ModuleDescription = "";

        #region Member Variables

        // flag that is set by client (via pipeline infrastructure call to Cancel) to abort operation of module
        protected bool stop = false;

        // master list of parameters (default SetParameters will build this)
        protected List<KeyValuePair<string, string>> parameters = new List<KeyValuePair<string, string>>();

        // master list of input column definitions
        // (default HandleColumnDef will build this)
        protected List<Dictionary<string, string>> InputColumnDefs = new List<Dictionary<string, string>>();
        //
        // master list of input column position keyed to column name (for lookup of column index by column name)
        // (default HandleColumnDef will build this)
        protected Dictionary<string, int> InputColumnPos = new Dictionary<string, int>();
        protected int InputColumnIndex = 0;

        // master list of Output column definitions
        // (not all modules require this feature)
        protected List<Dictionary<string, string>> OutputColumnDefs = null;
        //
        // master list of Output column position keyed to column name (for lookup of column index by column name)
        // (not all modules require this feature)
        protected Dictionary<string, int> OutputColumnPos = null;
        //
        // master list of position map between output columns and input columns
        protected List<KeyValuePair<int, int>> OutputToInputColumnPosMap = null;

        #endregion

        #region Properties

        // comma-delimited list of specs for output columns that the module will supply to standard tabular output
        // (this is only needed if module does not simply pass through the input columns)
        // Col Specs:
        // <output column name> - simple pass-through of input column with same name
        // <<output column name>|<input column name> - map input column to output column using different name
        // <output column name>|+|<type> - output column is new column
        public string OutputColumnList { get; set; }

        #endregion

        #region IBaseModule Members

        public virtual event DataRowHandler DataRowAvailable;
        public virtual event ColumnDefHandler ColumnDefAvailable;
        public virtual event StatusMessageUpdated OnStatusMessageUpdated;

        public string ModuleName { get; set; }

        public virtual void Prepare() {
            InputColumnIndex = 0;
            this.stop = false;
        }

        public virtual void Cleanup() {
        }

        // this implements the canonical mechanism for setting module parameters
        // first, parameters are captured in a master key/value list
        // next, the parameter list is traversed and any properties 
        // whose name matches a parameter's key are set with the parameter's value
        //
        public virtual void SetParameters(List<KeyValuePair<string, string>> parameters) {
            // add parameters to master list
            this.parameters.AddRange(parameters);
            // set properties (of subclasses) from parameters
            SetPropertiesFromParameters();
        }

        public virtual void HandleDataRow(object[] vals, ref bool stop) {
            throw new NotImplementedException();
        }

        public virtual void HandleColumnDef(Dictionary<string, string> columnDef) {
            if (columnDef != null) {
                try {
                    InputColumnPos.Add(columnDef["Name"].ToString(), InputColumnIndex++);
                    InputColumnDefs.Add(columnDef);
                } catch (Exception e) {
                    traceLog.Error("HandleColumnDef:" + e.Message);
                    throw new Exception("HandleColumnDef:" + e.Message);
                }
            } else {
                SetUpOutputColumns();
            }
        }

        public virtual void Run(object state) {
            throw new NotImplementedException();
        }

        public void Cancel() {
            this.stop = true;
        }

        #endregion


        #region helper functions

        // set properties (especially of subclasses) from parameters (by name using reflection)
        protected void SetPropertiesFromParameters() {
            foreach (KeyValuePair<string, string> paramDef in this.parameters) {
                PropertyInfo pi = this.GetType().GetProperty(paramDef.Key);
                if (pi != null) {
                    pi.SetValue(this, paramDef.Value, null);
                }
            }
        }

        // if there are any columns defined in the OutputColumnList property
        // populate the appropriate internal buffers with column definitions
        // and field indexes for them
        protected void SetUpOutputColumns() {
            if (OutputColumnList == null || OutputColumnList == "") return;

            OutputColumnPos = new Dictionary<string, int>();
            OutputColumnDefs = new List<Dictionary<string, string>>();
            OutputToInputColumnPosMap = new List<KeyValuePair<int, int>>();

            int outColIdx = 0;
            // process each column spec from spec list
            try {
                foreach (string colSpec in OutputColumnList.Split(',')) {
                    // break each column spec into fields
                    string[] colSpecFlds = colSpec.Trim().Split('|');
                    string outputColName = colSpecFlds[0].Trim();

                    if (colSpecFlds.Length == 1) {
                        // output column is simple pass-trough of input column
                        // copy input column def to output col def for this column
                        string inputColName = outputColName;
                        int inputColIdx = InputColumnPos[inputColName];
                        OutputColumnDefs.Add(InputColumnDefs[inputColIdx]);
                        OutputColumnPos.Add(outputColName, outColIdx);
                        OutputToInputColumnPosMap.Add(new KeyValuePair<int, int>(outColIdx, inputColIdx));
                        outColIdx++;
                    }
                    if (colSpecFlds.Length == 2) {
                        // output column is rename of input column
                        // copy input column def to output col def for this column
                        // and change the name of the column
                        string inputColName = colSpecFlds[1].Trim(); ;
                        int inputColIdx = InputColumnPos[inputColName];
                        Dictionary<string, string> colDef = InputColumnDefs[inputColIdx];
                        colDef["Name"] = outputColName;
                        OutputColumnDefs.Add(colDef);
                        OutputColumnPos.Add(outputColName, outColIdx);
                        OutputToInputColumnPosMap.Add(new KeyValuePair<int, int>(outColIdx, inputColIdx));
                        outColIdx++;
                    }
                    if (colSpecFlds.Length > 2) {
                        if (colSpecFlds[1] != "+") continue;
                        // output column is new column not found in input
                        // (module will supply value)
                        string type = colSpecFlds[2];
                        string size = (colSpecFlds.Length > 3) ? colSpecFlds[3] : "10";
                        OutputColumnDefs.Add(MakeColDef(outputColName, size, type));
                        OutputColumnPos.Add(outputColName, outColIdx);
                        outColIdx++;
                    }
                }
            } catch (Exception e) {
                traceLog.Error(e.Message);
                throw new Exception("Problem with defining output columns:" + e.Message);
            }
        }

        // if the module is using output column definition for output rows
        // (instead of defaulting to using the input column definition)
        // this function will create an output row according to output column
        // definiition
        protected object[] MapDataRow(object[] vals) {
            // remap results according to our output column definitions
            object[] outRow = new object[OutputColumnDefs.Count];
            foreach (KeyValuePair<int, int> colMap in OutputToInputColumnPosMap) {
                outRow[colMap.Key] = vals[colMap.Value];
            }
            return outRow;
        }


        // this creates a minimally acceptable, canonical, column definition description
        protected Dictionary<string, string> MakeColDef(string name, string size, string type) {
            Dictionary<string, string> myCol = new Dictionary<string, string>();
            myCol["Name"] = name;
            myCol["Size"] = size;
            myCol["Type"] = type;
            return myCol;
        }


        #endregion

    }
}
