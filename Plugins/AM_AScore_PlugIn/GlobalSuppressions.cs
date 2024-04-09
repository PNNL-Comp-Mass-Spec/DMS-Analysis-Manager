// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:AnalysisManager_AScore_PlugIn.AnalysisToolRunnerAScore.RunAScore~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0300:Simplify collection initialization", Justification = "Uses the preferred syntax", Scope = "member", Target = "~M:AnalysisManager_AScore_PlugIn.AScoreMagePipeline.GetListOfDataPackageJobsToProcess(System.String,System.String)~Mage.SimpleSink")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManager_AScore_PlugIn.AnalysisResourcesAScore")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManager_AScore_PlugIn.AnalysisToolRunnerAScore")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManager_AScore_PlugIn.AScoreMagePipeline")]
[assembly: SuppressMessage("Usage", "RCS1146:Use conditional access.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManager_AScore_PlugIn.MageAScoreModule.CopyDTAResultsFromServer(System.IO.DirectoryInfo,System.Int32,System.String,System.String)~System.String")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManager_AScore_PlugIn.MageAScoreModule.CopyDtaResultsFromMyEMSL(System.String,System.IO.FileSystemInfo,System.Int32,System.String,System.String)~System.String")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManager_AScore_PlugIn.MageAScoreModule.CopyDTAResultsFromServer(System.IO.DirectoryInfo,System.Int32,System.String,System.String)~System.String")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManager_AScore_PlugIn.MageAScoreModule.GetSharedResultsDirectoryName(System.Int32,System.String,System.String)~System.String")]
