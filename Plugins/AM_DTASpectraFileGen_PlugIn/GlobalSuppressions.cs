
// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:DTASpectraFileGen.DtaGenThermoRaw.UpdateDeconMSnProgress(System.String)")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:DTASpectraFileGen.DtaGenThermoRaw.UpdateDTAProgress(System.String)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:DTASpectraFileGen.DtaGenDeconConsole.ParseDeconToolsLogFile(System.Boolean@,System.DateTime@)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:DTASpectraFileGen.DtaGenThermoRaw.MakeDTAFiles~System.Boolean")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:DTASpectraFileGen.DtaGenToolRunner.StartAndWaitForDTAGenerator(AnalysisManagerBase.ISpectraFileProcessor,System.String,System.Boolean)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:DTASpectraFileGen.DtaGenDeconConsole.ParseDeconToolsLogFile(System.Boolean@,System.DateTime@)")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:DTASpectraFileGen.DtaGen")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:DTASpectraFileGen.DtaGenMSConvert")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:DTASpectraFileGen.DtaGenRawConverter")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:DTASpectraFileGen.DtaGenResources")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:DTASpectraFileGen.DtaGenToolRunner")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:DTASpectraFileGen.MGFConverter")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:DTASpectraFileGen.MGFtoDtaGenMainProcess")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:DTASpectraFileGen.DtaGenDeconConsole")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:DTASpectraFileGen.DtaGenThermoRaw")]
[assembly: SuppressMessage("Usage", "RCS1146:Use conditional access.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:DTASpectraFileGen.DtaGenToolRunner.RemoveTitleAndParentIonLines(System.String)~System.String")]
