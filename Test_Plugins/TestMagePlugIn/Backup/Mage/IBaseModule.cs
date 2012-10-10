using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mage {

    // delegate definitions for module events
    public delegate void DataRowHandler(object[] vals, ref bool stop);
    public delegate void ColumnDefHandler(Dictionary<string, string> columnDef);
    public delegate void StatusMessageUpdated(string Message);

    public interface IBaseModule {
        string ModuleName { get; set; }

        // called before pipeline runs - module can do any special setup that it needs
        void Prepare();
        // called after pipeline run is complete - module can do any special cleanup
        void Cleanup();

        void SetParameters(List<KeyValuePair<string, string>> parameters);

        // module delivers standard tablular output to registered listeners on these events
        void HandleDataRow(object[] vals, ref bool stop);
        void HandleColumnDef(Dictionary<string, string> columnDef);
 
        // Standard output stream
        event DataRowHandler DataRowAvailable;
        event ColumnDefHandler ColumnDefAvailable;


        event StatusMessageUpdated OnStatusMessageUpdated;

        // Pass execution to module if it does not respond standard input stream events
        // (for example, module that gets its data froma a database)
        void Run(Object state);

        // stop processing at clean break point
        void Cancel();
    }
}
