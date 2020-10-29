// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Acceptable", Scope = "module")]
[assembly: SuppressMessage("Performance", "RCS1197:Optimize StringBuilder.Append/AppendLine call.", Justification = "Acceptable", Scope = "module")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "module")]

[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsAnalysisJob")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsAnalysisMgrBase")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsAnalysisResources")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsAnalysisResources.eRawDataTypeConstants")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsAnalysisToolRunnerBase")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsDotNetZipTools")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsScanStatsGenerator")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsAnalysisResults")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsGlobal")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsGlobal.eAnalysisResourceOptions")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsCDTAUtilities")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsDirectorySearch")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerBase.clsFileCopyUtilities")]

[assembly: SuppressMessage("General", "RCS1079:Throwing of new NotImplementedException.", Justification = "This is a placeholder method; derived class will override it", Scope = "member", Target = "~M:AnalysisManagerBase.clsAnalysisToolRunnerBase.RetrieveRemoteResults(AnalysisManagerBase.clsRemoteTransferUtility,System.Boolean,System.Collections.Generic.List{System.String}@)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0059:Unnecessary assignment of a value", Justification = "Checking this value for safety", Scope = "member", Target = "~M:AnalysisManagerBase.clsGlobal.GetDataTableByCmd(System.Data.SqlClient.SqlCommand,System.String,System.Int16,System.Data.DataTable@,System.Int32,System.String)~System.Boolean")]
