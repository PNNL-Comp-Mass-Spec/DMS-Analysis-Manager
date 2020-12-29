// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore a file deletion error in this case", Scope = "member", Target = "~M:AnalysisManagerMzRefineryPlugIn.clsAnalysisToolRunnerMzRefinery.CopyFailedResultsToArchiveFolder(System.String)")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.clsAnalysisResourcesMzRefinery")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.clsAnalysisToolRunnerMzRefinery")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.clsAnalysisToolRunnerMzRefinery.eMzRefinerProgRunnerMode")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.clsMzRefineryMassErrorStatsExtractor")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.clsMassErrorInfo")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
