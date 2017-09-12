using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security;
using log4net.Core;
using log4net.Layout;
using log4net.Layout.Pattern;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Custom formatter for LogForNET
    /// Displays the full stack trace when the calling method includes an exception
    /// </summary>
    /// <remarks>
    /// Modelled after code at http://stackoverflow.com/questions/1906227/does-log4net-support-including-the-call-stack-in-a-log-message
    /// </remarks>
    public class CustomPatternLayout : PatternLayout
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CustomPatternLayout()
        {
            AddConverter("stack", typeof(StackTraceConverter));
        }
    }

    internal sealed class StackTraceConverter : PatternLayoutConverter
    {

        private static readonly Assembly _assembly = typeof(PatternLayoutConverter).Assembly;
        public bool StackTraceIncludesFilenames { get; }

        /// <summary>
        /// When true, include method arguments in the stack trace
        /// </summary>
        /// <returns></returns>
        public bool StackTraceIncludesMethodArgs { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public StackTraceConverter()
        {
            IgnoresException = false;
            StackTraceIncludesFilenames = true;
            StackTraceIncludesMethodArgs = false;
        }

        /// <summary>
        /// Appends details about the exception to the writer
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="loggingEvent"></param>
        protected override void Convert(TextWriter writer, LoggingEvent loggingEvent)
        {
            var ex = loggingEvent.ExceptionObject;
            if (ex == null)
            {
                return;
            }

            var displayFilenames = StackTraceIncludesFilenames;
            var exceptionWritten = false;

            // Setting this to 3 prevents the stack trace from including this method, the calling method, or the method above that
            const int parentMethodsToSkip = 3;

            try
            {
                // Note that the request for filenames (when displayFilenames is true) may fail
                var stack = new StackTrace(displayFilenames);

                string basePath;
                if (displayFilenames)
                {
                    basePath = GetBasePath(stack, parentMethodsToSkip);
                    // When writing the exception, replace basePath with an empty string
                    // Also replacing " in \" with " in "
                    writer.WriteLine(ex.ToString().Replace(basePath, string.Empty).Replace(" in " + Path.DirectorySeparatorChar, " in "));
                }
                else
                {
                    basePath = string.Empty;
                    writer.WriteLine(ex.ToString());
                }
                exceptionWritten = true;

                var skip = 0;
                for (var i = 0; i <= stack.FrameCount - 1; i++)
                {
                    var sf = stack.GetFrame(i);
                    var mb = sf.GetMethod();
                    if (mb == null)
                        continue;

                    var t = mb.DeclaringType;
                    if (t == null)
                        continue;

                    if (t.Assembly == _assembly)
                        continue;

                    // This skips the current method and the method catching the exception
                    // parentMethodsToSkip will typically be 2 or 3
                    if (skip < parentMethodsToSkip)
                    {
                        skip += 1;
                        continue;
                    }
                    writer.Write("   at ");

                    // if there is a type (non global method) print it
                    if (t.FullName != null)
                        writer.Write(t.FullName.Replace('+', '.'));

                    writer.Write(".");
                    writer.Write(mb.Name);

                    // deal with the generic portion of the method
                    var info = mb as MethodInfo;
                    if (info != null && info.IsGenericMethod)
                    {
                        var typars = info.GetGenericArguments();
                        writer.Write("[");
                        var k = 0;
                        var fFirstTyParam = true;
                        while (k < typars.Length)
                        {
                            if (fFirstTyParam == false)
                            {
                                writer.Write(",");
                            }
                            else
                            {
                                fFirstTyParam = false;
                            }

                            writer.Write(typars[k].Name);
                            k += 1;
                        }
                        writer.Write("]");
                    }

                    // Optionally include method arguments
                    if (StackTraceIncludesMethodArgs)
                    {
                        writer.Write("(");
                        var pi = mb.GetParameters();
                        var fFirstParam = true;
                        for (var j = 0; j <= pi.Length - 1; j++)
                        {
                            if (fFirstParam == false)
                            {
                                writer.Write(", ");
                            }
                            else
                            {
                                fFirstParam = false;
                            }

                            var typeName = pi[j].ParameterType.Name;
                            writer.Write(typeName + " " + pi[j].Name);
                        }
                        writer.Write(")");
                    }

                    // source location printing
                    if (displayFilenames && (sf.GetILOffset() != -1))
                    {
                        // If we don't have a PDB or PDB-reading is disabled for the module, the file name will be null
                        string filePath = null;

                        // Getting the filename from a StackFrame is a privileged operation
                        // We won't want to disclose full path names to arbitrarily untrusted code.
                        // Rather than just omit this we could probably trim to just the filename so it's still mostly useful
                        try
                        {
                            filePath = sf.GetFileName();
                        }
                        catch (SecurityException)
                        {
                            // If the demand for displaying filenames fails, it won't succeed later in the loop.
                            // Avoid repeated exceptions by not trying again.
                            displayFilenames = false;
                        }

                        if (filePath != null)
                        {
                            string trimmedFilePath;
                            if (filePath.StartsWith(basePath, StringComparison.OrdinalIgnoreCase))
                            {
                                if (filePath.Substring(basePath.Length).StartsWith(Path.DirectorySeparatorChar.ToString()) ||
                                    filePath.Substring(basePath.Length).StartsWith(Path.AltDirectorySeparatorChar.ToString()))
                                {
                                    trimmedFilePath = filePath.Substring(basePath.Length + 1);
                                }
                                else
                                {
                                    trimmedFilePath = filePath.Substring(basePath.Length);
                                }
                            }
                            else
                            {
                                trimmedFilePath = filePath;
                            }

                            // Append " in c:\tmp\MyFile.cs:line 5"
                            writer.Write(" in {0}:line {1}", trimmedFilePath, sf.GetFileLineNumber());
                        }
                    }
                    writer.WriteLine();

                }

            }
            catch (Exception ex2)
            {
                writer.WriteLine();
                if (!exceptionWritten)
                {
                    writer.WriteLine(ex.ToString());
                }

                writer.WriteLine("Exception logging the exception: " + ex2.Message);
            }

        }

        /// <summary>
        /// Examine the file paths associated with each level of the stack trace
        /// Determine the folder path in common with all of the file paths
        /// </summary>
        /// <param name="stack"></param>
        /// <param name="parentMethodsToSkip"></param>
        /// <returns>Base folder path</returns>
        private string GetBasePath(StackTrace stack, int parentMethodsToSkip)
        {

            // This list tracks the file paths associated with the methods in the call stack
            var stackTraceFiles = new List<string>();

            var skip = 0;
            for (var i = 0; i <= stack.FrameCount - 1; i++)
            {
                var sf = stack.GetFrame(i);
                var mb = sf.GetMethod();
                if (mb == null)
                    continue;

                var t = mb.DeclaringType;

                if (t == null)
                    continue;

                if (t.Assembly == _assembly)
                    continue;

                // This skips the current method plus any parent methods
                if (skip < 1 + parentMethodsToSkip)
                {
                    skip += 1;
                    continue;
                }

                if (sf.GetILOffset() != -1)
                {
                    // If we don't have a PDB or PDB-reading is disabled for the module, the file name will be null
                    string filePath;

                    try
                    {
                        filePath = sf.GetFileName();
                    }
                    catch (SecurityException)
                    {
                        // If the demand for displaying filenames fails, it won't succeed later in the loop.
                        // Abort looking up filenames for methods in the call stack
                        break;
                    }

                    if (filePath != null)
                    {
                        stackTraceFiles.Add(filePath);
                    }
                }

            }

            if (stackTraceFiles.Count == 0)
            {
                return string.Empty;
            }

            // Find the path portion that is in common with all of the files in stackTraceFiles
            stackTraceFiles.Sort();

            var sourceFileFirst = new FileInfo(stackTraceFiles.First());

            var basePath = sourceFileFirst.Directory.FullName;

            if (stackTraceFiles.Count == 1)
            {
                return basePath;
            }

            foreach (var filePath in stackTraceFiles)
            {
                var sourceFile = new FileInfo(filePath);

                var basePathCompare = sourceFile.Directory.FullName;
                var charsToCompare = Math.Min(basePath.Length, basePathCompare.Length);
                var i = 0;

                if (string.Equals(basePath.Substring(0, charsToCompare), basePathCompare.Substring(0, charsToCompare), StringComparison.OrdinalIgnoreCase))
                {
                    // The path portions are equal; no need for character-by-character comparison
                    i = charsToCompare;

                }
                else
                {
                    while (i < charsToCompare)
                    {
                        if (basePath[i] != basePathCompare[i])
                        {
                            // Difference found
                            break;
                        }
                        i += 1;
                    }

                }

                if (i < basePath.Length)
                {
                    basePath = basePath.Substring(0, i);
                }
            }

            return basePath;

        }
    }

}