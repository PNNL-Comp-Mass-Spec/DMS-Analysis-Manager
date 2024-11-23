// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Leave for reference", Scope = "member", Target = "~M:AnalysisManagerBase.JobConfig.AssemblyTools.GetLoadedAssemblyInfo(AnalysisManagerBase.JobConfig.SummaryFile)")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Acceptable", Scope = "module")]
[assembly: SuppressMessage("Design", "RCS1194:Implement exception constructors.", Justification = "Extra constructors are not needed", Scope = "module")]
[assembly: SuppressMessage("General", "RCS1079:Throwing of new NotImplementedException.", Justification = "This is a placeholder method; derived class will override it", Scope = "member", Target = "~M:AnalysisManagerBase.AnalysisTool.AnalysisResources.CopyResourcesToRemote(AnalysisManagerBase.OfflineJobs.RemoteTransferUtility)~System.Boolean")]
[assembly: SuppressMessage("General", "RCS1079:Throwing of new NotImplementedException.", Justification = "This is a placeholder method; derived class will override it", Scope = "member", Target = "~M:AnalysisManagerBase.AnalysisTool.AnalysisToolRunnerBase.RetrieveRemoteResults(AnalysisManagerBase.OfflineJobs.RemoteTransferUtility,System.Boolean,System.Collections.Generic.List{System.String}@)~System.Boolean")]
[assembly: SuppressMessage("Performance", "RCS1197:Optimize StringBuilder.Append/AppendLine call.", Justification = "Acceptable", Scope = "module")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "module")]
[assembly: SuppressMessage("Roslynator", "RCS1146:Use conditional access", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.FileAndDirectoryTools.ZipFileTools.UnzipFiles(System.IO.Compression.ZipArchive,System.String,System.String,AnalysisManagerBase.FileAndDirectoryTools.ZipFileTools.ExtractExistingFileBehavior)")]
[assembly: SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.FileAndDirectoryTools.FileSearch.FindNewestMsXmlFileInCache(AnalysisManagerBase.AnalysisTool.AnalysisResources.MSXMLOutputTypeConstants,System.String@)~System.String")]
[assembly: SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.OfflineJobs.RemoteMonitor.ParseJobStatusFile(System.String)~AnalysisManagerBase.OfflineJobs.RemoteMonitor.RemoteJobStatusCodes")]
[assembly: SuppressMessage("Style", "IDE0270:Use coalesce expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.DataFileTools.FastaFileUtilities.RetrieveDecoyFASTA(System.IO.FileSystemInfo,System.String@)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0270:Use coalesce expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.DataFileTools.ProteowizardTools.RegisterProteoWizardWindows~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0270:Use coalesce expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.DataFileTools.SplitFastaFileUtilities.ValidateSplitFastaFile(System.String,System.String)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0305:Simplify collection initialization", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.AnalysisTool.AnalysisToolRunnerBase.ExtractPackedJobParameterList(System.String)~System.Collections.Generic.List{System.String}")]
[assembly: SuppressMessage("Style", "IDE0305:Simplify collection initialization", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.DataFileTools.DatasetNameMapUtility.GetDatasetNameMap(System.Collections.Generic.SortedSet{System.String},System.String@,System.Collections.Generic.List{System.String}@)~System.Collections.Generic.Dictionary{System.String,System.String}")]
[assembly: SuppressMessage("Style", "IDE0305:Simplify collection initialization", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerBase.JobConfig.AnalysisJob.GetAllSectionNames~System.Collections.Generic.List{System.String}")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerBase.DBTask")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerBase.Global")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerBase.Global.AnalysisResourceOptions")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
