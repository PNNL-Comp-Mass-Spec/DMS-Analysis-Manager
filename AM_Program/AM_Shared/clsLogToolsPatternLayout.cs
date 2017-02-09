Imports System.IO
Imports System.Reflection
Imports System.Security
Imports log4net.Core
Imports log4net.Layout
Imports log4net.Layout.Pattern

''' <summary>
''' Custom formatter for LogForNET
''' Displays the full stack trace when the calling method includes an exception
''' </summary>
''' <remarks>
''' Modelled after code at http://stackoverflow.com/questions/1906227/does-log4net-support-including-the-call-stack-in-a-log-message
''' </remarks>
Public Class CustomPatternLayout
    Inherits PatternLayout
    Public Sub New()
        Me.AddConverter("stack", GetType(StackTraceConverter))
    End Sub
End Class

Friend Class StackTraceConverter
    Inherits PatternLayoutConverter
    Private Shared ReadOnly _assembly As Assembly = GetType(PatternLayoutConverter).Assembly

    Public ReadOnly Property StackTraceIncludesFilenames As Boolean

    ''' <summary>
    ''' When true, include method arguments in the stack trace
    ''' </summary>
    ''' <returns></returns>
    Public ReadOnly Property StackTraceIncludesMethodArgs As Boolean

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New()
        MyBase.IgnoresException = False
        StackTraceIncludesFilenames = True
        StackTraceIncludesMethodArgs = False
    End Sub

    ''' <summary>
    ''' Appends details about the exception to the writer
    ''' </summary>
    ''' <param name="writer"></param>
    ''' <param name="loggingEvent"></param>
    Protected Overrides Sub Convert(writer As TextWriter, loggingEvent As LoggingEvent)
        Dim ex = loggingEvent.ExceptionObject
        If ex Is Nothing Then
            Return
        End If

        Dim displayFilenames = StackTraceIncludesFilenames
        Dim exceptionWritten = False

        ' Setting this to 3 prevents the stack trace from including this method, the calling method, or the method above that
        Const parentMethodsToSkip = 3

        Try
            ' Note that the request for filenames (when displayFilenames is true) may fail
            Dim stack = New StackTrace(displayFilenames)

            Dim basePath As String
            If displayFilenames Then
                basePath = GetBasePath(stack, parentMethodsToSkip)
                ' When writing the exception, replace basePath with an empty string
                ' Also replacing " in \" with " in "
                writer.WriteLine(ex.ToString().Replace(basePath, String.Empty).Replace(" in " & Path.DirectorySeparatorChar, " in "))
            Else
                basePath = String.Empty
                writer.WriteLine(ex.ToString())
            End If
            exceptionWritten = True

            Dim skip = 0
            For i = 0 To stack.FrameCount - 1
                Dim sf = stack.GetFrame(i)
                Dim mb = sf.GetMethod()
                If mb Is Nothing Then Continue For

                Dim t = mb.DeclaringType
                If t.Assembly = _assembly Then Continue For

                ' This skips the current method and the method catching the exception
                ' parentMethodsToSkip will typically be 2 or 3
                If skip < parentMethodsToSkip Then
                    skip += 1
                    Continue For
                End If
                writer.Write("   at ")

                ' if there is a type (non global method) print it
                If t IsNot Nothing Then
                    writer.Write(t.FullName.Replace("+"c, "."c))
                    writer.Write(".")
                End If
                writer.Write(mb.Name)

                ' deal with the generic portion of the method
                If TypeOf mb Is MethodInfo AndAlso mb.IsGenericMethod Then
                    Dim typars As Type() = DirectCast(mb, MethodInfo).GetGenericArguments()
                    writer.Write("[")
                    Dim k = 0
                    Dim fFirstTyParam = True
                    While k < typars.Length
                        If fFirstTyParam = False Then
                            writer.Write(",")
                        Else
                            fFirstTyParam = False
                        End If

                        writer.Write(typars(k).Name)
                        k += 1
                    End While
                    writer.Write("]")
                End If

                ' Optionally include method arguments
                If StackTraceIncludesMethodArgs Then
                    writer.Write("(")
                    Dim pi As ParameterInfo() = mb.GetParameters()
                    Dim fFirstParam = True
                    For j = 0 To pi.Length - 1
                        If fFirstParam = False Then
                            writer.Write(", ")
                        Else
                            fFirstParam = False
                        End If

                        Dim typeName = "<UnknownType>"
                        If pi(j).ParameterType IsNot Nothing Then
                            typeName = pi(j).ParameterType.Name
                        End If
                        writer.Write(typeName + " " + pi(j).Name)
                    Next
                    writer.Write(")")
                End If

                ' source location printing
                If displayFilenames AndAlso (sf.GetILOffset() <> -1) Then
                    ' If we don't have a PDB or PDB-reading is disabled for the module, the file name will be null
                    Dim filePath As String = Nothing

                    ' Getting the filename from a StackFrame is a privileged operation
                    ' We won't want to disclose full path names to arbitrarily untrusted code.  
                    ' Rather than just omit this we could probably trim to just the filename so it's still mostly useful
                    Try
                        filePath = sf.GetFileName()
                    Catch generatedExceptionName As SecurityException
                        ' If the demand for displaying filenames fails, it won't succeed later in the loop.  
                        ' Avoid repeated exceptions by not trying again.
                        displayFilenames = False
                    End Try

                    If filePath IsNot Nothing Then
                        Dim trimmedFilePath As String
                        If filePath.StartsWith(basePath, StringComparison.InvariantCultureIgnoreCase) Then
                            If filePath.Substring(basePath.Length).StartsWith(Path.DirectorySeparatorChar) OrElse
                               filePath.Substring(basePath.Length).StartsWith(Path.AltDirectorySeparatorChar) Then
                                trimmedFilePath = filePath.Substring(basePath.Length + 1)
                            Else
                                trimmedFilePath = filePath.Substring(basePath.Length)
                            End If
                        Else
                            trimmedFilePath = filePath
                        End If

                        ' Append " in c:\tmp\MyFile.cs:line 5"
                        writer.Write(" in {0}:line {1}", trimmedFilePath, sf.GetFileLineNumber())
                    End If
                End If
                writer.WriteLine()

            Next

        Catch ex2 As Exception
            writer.WriteLine()
            If Not exceptionWritten Then
                writer.WriteLine(ex.ToString())
            End If

            writer.WriteLine("Exception logging the exception: " & ex2.Message)
        End Try

    End Sub

    ''' <summary>
    ''' Examine the file paths associated with each level of the stack trace
    ''' Determine the folder path in common with all of the file paths
    ''' </summary>
    ''' <param name="stack"></param>
    ''' <param name="parentMethodsToSkip"></param>
    ''' <returns>Base folder path</returns>
    Private Function GetBasePath(stack As StackTrace, parentMethodsToSkip As Integer) As String

        ' This list tracks the file paths associated with the methods in the call stack
        Dim stackTraceFiles = New List(Of String)

        Dim skip = 0
        For i = 0 To stack.FrameCount - 1
            Dim sf = stack.GetFrame(i)
            Dim mb = sf.GetMethod()
            If mb Is Nothing Then Continue For

            Dim t = mb.DeclaringType
            If t.Assembly = _assembly Then Continue For

            ' This skips the current method plus any parent methods
            If skip < 1 + parentMethodsToSkip Then
                skip += 1
                Continue For
            End If

            If (sf.GetILOffset() <> -1) Then
                ' If we don't have a PDB or PDB-reading is disabled for the module, the file name will be null
                Dim filePath As String

                Try
                    filePath = sf.GetFileName()
                Catch generatedExceptionName As SecurityException
                    ' If the demand for displaying filenames fails, it won't succeed later in the loop.  
                    ' Abort looking up filenames for methods in the call stack
                    Exit For
                End Try

                If filePath IsNot Nothing Then
                    stackTraceFiles.Add(filePath)
                End If
            End If

        Next

        If stackTraceFiles.Count = 0 Then
            Return String.Empty
        End If

        ' Find the path portion that is in common with all of the files in stackTraceFiles
        stackTraceFiles.Sort()

        Dim sourceFileFirst = New FileInfo(stackTraceFiles.First())
        Dim basePath = sourceFileFirst.Directory.FullName

        If stackTraceFiles.Count = 1 Then
            Return basePath
        End If

        For Each filePath In stackTraceFiles
            Dim sourceFile = New FileInfo(filePath)
            Dim basePathCompare = sourceFile.Directory.FullName
            Dim charsToCompare = Math.Min(basePath.Length, basePathCompare.Length)
            Dim i = 0

            If String.Equals(basePath.Substring(0, charsToCompare), basePathCompare.Substring(0, charsToCompare), StringComparison.InvariantCultureIgnoreCase) Then
                ' The path portions are equal; no need for character-by-character comparison
                i = charsToCompare
            Else

                While i < charsToCompare
                    If basePath(i) <> basePathCompare(i) Then
                        ' Difference found
                        Exit While
                    End If
                    i += 1
                End While

            End If

            If i < basePath.Length Then
                basePath = basePath.Substring(0, i)
            End If
        Next

        Return basePath

    End Function
End Class
