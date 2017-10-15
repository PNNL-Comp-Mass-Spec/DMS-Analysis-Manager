
using System;
using System.Diagnostics;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 04/30/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Class for logging of problems prior to manager's full logging capability being available
    /// </summary>
    [Obsolete("Unused")]
    public class clsEmergencyLog
    {

        #region "Methods"

        /// <summary>
        /// Writes a message to a custom event log, which is used if standard log file not available
        /// </summary>
        /// <param name="SourceName">Name of source (program) using log</param>
        /// <param name="LogName">Name of log</param>
        /// <param name="ErrMsg">Message to write to log</param>
        /// <remarks></remarks>
        public static void WriteToLog(string SourceName, string LogName, string ErrMsg)
        {
            // If custom event log doesn't exist yet, create it
            if (!EventLog.SourceExists(SourceName))
            {
                var SourceData = new EventSourceCreationData(SourceName, LogName);
                EventLog.CreateEventSource(SourceData);
            }

            // Create custom event logging object and write to log
            var ELog = new EventLog
            {
                Log = LogName,
                Source = SourceName,
                MaximumKilobytes = 1024
            };
            ELog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90);
            EventLog.WriteEntry(SourceName, ErrMsg, EventLogEntryType.Error);

        }
        #endregion

    }

}
