using System;

namespace AnalysisManagerDiaNNPlugIn
{
    /// <summary>
    /// Metadata for a static or dynamic post-translational modification
    /// </summary>
    /// <remarks>
    /// Do not use this class to define static Cys carbamidomethyl (+57.021), aka iodoacetamide alkylation
    /// Instead, set StaticCysCarbamidomethyl to true so that --unimod4 is included in the argument list sent to DiaNN.exe
    /// </remarks>
    public class ModificationInfo
    {
        // Ignore Spelling: DIA

        /// <summary>
        /// Affected residues or peptide/protein position
        /// </summary>
        public string AffectedResidues { get; }

        /// <summary>
        /// True if a fixed mod with "label" at the end of the modification definition, e.g. SILAC,0.0,KR,label
        /// </summary>
        public bool IsFixedLabelMod { get; }

        /// <summary>
        /// When not an empty string, append an argument of the form "--lib-fixed-mod SILAC" to the argument list sent to DiaNN.exe
        /// </summary>
        /// <remarks>Only valid for static modifications of type "label"</remarks>
        public string LibFixedMod { get; set; }

        /// <summary>
        /// Modification definition: comma separated list of modification name (e.g. UniMod:35), modification mass, and affected residues
        /// </summary>
        /// <remarks>
        /// <para>
        /// Dynamic (variable) mod examples:
        ///   UniMod:35,15.994915,M
        ///   UniMod:1,42.010565,*n
        ///   UniMod:21,79.966331,STY
        ///   UniMod:121,114.042927,K
        /// </para>
        /// <para>
        /// Static (fixed) mod example:
        ///   SILAC,0.0,KR,label
        /// </para>
        /// </remarks>
        public string ModificationDefinition { get; }

        /// <summary>
        /// Modification mass
        /// </summary>
        public double ModificationMass { get; }

        /// <summary>
        /// Modification name
        /// </summary>
        public string ModificationName { get; }

        /// <summary>
        /// Modification type
        /// </summary>
        public ModificationTypes ModificationType { get; }

        /// <summary>
        /// When true, disable scoring and localization for this modification
        /// </summary>
        /// <remarks>
        /// <para>Introduced in DIA-NN 1.9</para>
        /// </remarks>
        public bool DisableScoring { get; set; }

        // ReSharper disable once UnusedMember.Global

        /// <summary>
        /// When true, append an argument of the form "--monitor-mod UniMod:121" to the argument list sent to DiaNN.exe
        /// Enables PTM scoring & site localisation for the given mode
        /// </summary>
        /// <remarks>
        /// <para>Present in DIA-NN 1.8 but removed in DIA-NN 1.9</para>
        /// <para>Only valid for dynamic modifications</para>
        /// </remarks>
        [Obsolete("Removed in DIA-NN 1.9")]
        public bool MonitorMod { get; set; }

        /// <summary>
        /// When true, append an argument of the form "--no-cut-after-mod UniMod:121"
        /// </summary>
        /// <remarks>Only valid for dynamic modifications</remarks>
        public bool NoCutAfterMod { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="modificationType">Modification type</param>
        /// <param name="modificationDefinition">Modification definition, e.g. UniMod:35,15.994915,M</param>
        /// <param name="modificationName">Modification name, e.g. UniMod:35</param>
        /// <param name="modificationMass">Modification mass, e.g. 15.994915</param>
        /// <param name="affectedResidues">Affected residues (amino acid symbol or peptide/protein position), e.g. M</param>
        /// <param name="isFixedLabelMod">True if a fixed mod with "label" at the end of the modification definition, e.g. SILAC,0.0,KR,label</param>
        public ModificationInfo(
            ModificationTypes modificationType,
            string modificationDefinition,
            string modificationName,
            double modificationMass,
            string affectedResidues,
            bool isFixedLabelMod = false)
        {
            ModificationType = modificationType;
            ModificationDefinition = modificationDefinition;
            ModificationName = modificationName;
            ModificationMass = modificationMass;
            AffectedResidues = affectedResidues;
            IsFixedLabelMod = isFixedLabelMod;

            AutoDefineOptions();
        }

        private void AutoDefineOptions()
        {
            if (ModificationType == ModificationTypes.Dynamic)
            {
                /*
                 * Removed in DIA-NN 1.9
                 *
                    if (MatchesModNameOrMass("UniMod:35", 15.994915))
                    {
                        // Do not enable MonitorMod for Oxidized methionine
                    }
                    else
                    {
                        MonitorMod = true;
                    }
                */

                if (MatchesModNameOrMass("UniMod:121", 114.042927))
                {
                    // ReSharper disable once CommentTypo
                    // Lysine ubiquitinylation
                    NoCutAfterMod = true;
                }
            }
            else if (IsFixedLabelMod)
            {
                LibFixedMod = string.Format("--lib-fixed-mod {0}", ModificationName);
            }
        }

        /// <summary>
        /// Returns an instance of this class where the modification type is static, the definition is empty, and the name is "undefined"
        /// </summary>
        public static ModificationInfo GetUndefinedModification()
        {
            return new ModificationInfo(ModificationTypes.Static, string.Empty, "undefined", 0, string.Empty);
        }

        /// <summary>
        /// Return true if property ModificationName matches modificationName or if property ModificationMass is within tolerance of modificationMass
        /// </summary>
        /// <param name="modificationName">Modification name</param>
        /// <param name="modificationMass">Modification mass</param>
        /// <param name="massTolerance">Mass tolerance</param>
        private bool MatchesModNameOrMass(string modificationName, double modificationMass, double massTolerance = 0.00125)
        {
            return ModificationName.Equals(modificationName, StringComparison.OrdinalIgnoreCase) ||
                   Math.Abs(ModificationMass - modificationMass) <= massTolerance;
        }

        /// <summary>
        /// Show the modification type, name, and mass
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0} {1}: {2}", ModificationType, ModificationName ?? "<Undefined>", ModificationMass);
        }
    }
}
