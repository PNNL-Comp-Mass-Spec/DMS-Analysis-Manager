// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:AnalysisManagerFormularityPlugin.AnalysisToolRunnerFormularity.CopyFailedResultsToArchiveDirectory")]
[assembly: SuppressMessage("Style", "IDE0305:Simplify collection initialization", Justification = "Uses the preferred syntax", Scope = "member", Target = "~M:AnalysisManagerFormularityPlugin.AnalysisToolRunnerFormularity.GetXmlSpectraFiles(System.IO.DirectoryInfo,System.String@)~System.Collections.Generic.List{System.IO.FileInfo}")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerFormularityPlugin.AnalysisToolRunnerFormularity")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerFormularityPlugin.AnalysisResourcesFormularity")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerFormularityPlugin.AnalysisToolRunnerFormularity.PostProcessResults(System.String)~AnalysisManagerBase.JobConfig.CloseOutType")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerFormularityPlugin.PngToPdfConverter.CreatePdf(System.String,System.Collections.Generic.List{System.IO.FileInfo},System.String)~System.Boolean")]
[assembly: SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Leave for reference", Scope = "member", Target = "~M:AnalysisManagerFormularityPlugin.AnalysisToolRunnerFormularity.CreateZipFileWithPlotsAndHTML(System.IO.FileSystemInfo,System.Collections.Generic.IReadOnlyCollection{System.IO.FileInfo})~AnalysisManagerBase.JobConfig.CloseOutType")]
