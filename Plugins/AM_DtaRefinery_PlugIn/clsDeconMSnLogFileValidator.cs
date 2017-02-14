Option Strict On

Imports System.Collections.Generic
Imports System.IO
Imports System.Runtime.InteropServices
Imports System.Text
Imports System.Threading

''' <summary>
''' This class can be used to validate the data in a _DeconMSn_log.txt file
''' It makes sure that the intensity values in the last two columns are all > 0
''' Values that are 0 are auto-changed to 1
''' </summary>
''' <remarks></remarks>
Public Class clsDeconMSnLogFileValidator

    Private mErrorMessage As String = String.Empty
    Private mFileUpdated As Boolean

    ''' <summary>
    ''' Error message (if any)
    ''' </summary>
    Public ReadOnly Property ErrorMessage As String
        Get
            Return mErrorMessage
        End Get
    End Property

    ''' <summary>
    ''' Indicates whether the intensity values in the original file were updated
    ''' </summary>
    ''' <returns>True if the file was updated</returns>
    Public ReadOnly Property FileUpdated As Boolean
        Get
            Return mFileUpdated
        End Get
    End Property

    Private Function CollapseLine(strSplitLine() As String) As String
        Dim sbCollapsed As New StringBuilder(1024)

        If strSplitLine.Length > 0 Then
            sbCollapsed.Append(strSplitLine(0))
            For intIndex = 1 To strSplitLine.Length - 1
                sbCollapsed.Append(ControlChars.Tab & strSplitLine(intIndex))
            Next
        End If

        Return sbCollapsed.ToString()
    End Function

    ''' <summary>
    ''' Parse the specified DeconMSn log file to check for intensity values in the last two columns that are zero
    ''' </summary>
    ''' <param name="strSourceFilePath">Path to the file</param>
    ''' <returns>True if success; false if an unrecoverable error</returns>
    Public Function ValidateDeconMSnLogFile(strSourceFilePath As String) As Boolean

        Dim strTempFilePath As String

        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim blnHeaderPassed = False
        Dim blnColumnUpdated As Boolean

        Dim intParentIntensityColIndex = 9
        Dim intMonoIntensityColIndex = 10
        Dim intColumnCountUpdated As Integer

        Try

            mErrorMessage = String.Empty
            mFileUpdated = False

            Using srSourceFile = New StreamReader(New FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                strTempFilePath = Path.GetTempFileName()
                Thread.Sleep(250)

                Using swOutFile = New StreamWriter(New FileStream(strTempFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))

                    Do While Not srSourceFile.EndOfStream
                        strLineIn = srSourceFile.ReadLine()
                        intColumnCountUpdated = 0

                        If blnHeaderPassed Then
                            strSplitLine = strLineIn.Split(ControlChars.Tab)

                            If strSplitLine.Length > 1 AndAlso strSplitLine(0) = "MSn_Scan" Then
                                ' This is the header line
                                ValidateHeader(strLineIn, intParentIntensityColIndex, intMonoIntensityColIndex)
                            ElseIf strSplitLine.Length > 1 Then
                                ValidateColumnIsPositive(strSplitLine, intParentIntensityColIndex, blnColumnUpdated)
                                If blnColumnUpdated Then intColumnCountUpdated += 1

                                ValidateColumnIsPositive(strSplitLine, intMonoIntensityColIndex, blnColumnUpdated)
                                If blnColumnUpdated Then intColumnCountUpdated += 1

                            End If

                            If intColumnCountUpdated > 0 Then
                                mFileUpdated = True
                                swOutFile.WriteLine(CollapseLine(strSplitLine))
                            Else
                                swOutFile.WriteLine(strLineIn)
                            End If

                        Else
                            If strLineIn.StartsWith("--------------") Then
                                blnHeaderPassed = True
                            ElseIf strLineIn.StartsWith("MSn_Scan") Then
                                ValidateHeader(strLineIn, intParentIntensityColIndex, intMonoIntensityColIndex)
                                blnHeaderPassed = True
                            End If
                            swOutFile.WriteLine(strLineIn)
                        End If
                    Loop

                End Using

            End Using

            If mFileUpdated Then
                ' First rename strFilePath
                Dim ioFileInfo = New FileInfo(strSourceFilePath)
                Dim strTargetFilePath As String = Path.Combine(ioFileInfo.DirectoryName, Path.GetFileNameWithoutExtension(ioFileInfo.Name) & "_Original.txt")

                If File.Exists(strTargetFilePath) Then
                    Try
                        File.Delete(strTargetFilePath)
                    Catch ex As Exception
                        mErrorMessage = "Error deleting old _Original.txt file: " & ex.Message
                        Console.WriteLine(mErrorMessage)
                    End Try
                End If

                Try
                    ioFileInfo.MoveTo(strTargetFilePath)

                    ' Now copy the temp file to strFilePath
                    File.Copy(strTempFilePath, strSourceFilePath, False)

                Catch ex As Exception
                    mErrorMessage = "Error replacing source file with new file: " & ex.Message
                    Console.WriteLine(mErrorMessage)

                    ' Copy the temp file to strFilePath
                    File.Copy(strTempFilePath, Path.Combine(ioFileInfo.DirectoryName, Path.GetFileNameWithoutExtension(ioFileInfo.Name) & "_New.txt"), True)
                    File.Delete(strTempFilePath)

                    Return False
                End Try

            End If

            File.Delete(strTempFilePath)

        Catch ex As Exception
            mErrorMessage = "Exception in clsDeconMSnLogFileValidator.ValidateFile: " & ex.Message
            Console.WriteLine(mErrorMessage)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Validate the header, updating the column indices if necessary
    ''' </summary>
    ''' <param name="strLineIn"></param>
    ''' <param name="intParentIntensityColIndex">Input/output parameter</param>
    ''' <param name="intMonoIntensityColIndex">Input/output parameter</param>
    ''' <remarks></remarks>
    Private Sub ValidateHeader(strLineIn As String, ByRef intParentIntensityColIndex As Integer, ByRef intMonoIntensityColIndex As Integer)
        Dim strSplitLine() As String
        Dim lstSplitLine As List(Of String)
        Dim intColIndex As Integer

        strSplitLine = strLineIn.Split(ControlChars.Tab)

        If strSplitLine.Length > 1 Then
            lstSplitLine = New List(Of String)(strSplitLine)

            intColIndex = lstSplitLine.IndexOf("Parent_Intensity")
            If intColIndex > 0 Then intParentIntensityColIndex = intColIndex

            intColIndex = lstSplitLine.IndexOf("Mono_Intensity")
            If intColIndex > 0 Then intMonoIntensityColIndex = intColIndex
        End If

    End Sub

    Private Sub ValidateColumnIsPositive(strSplitLine() As String, intColIndex As Integer, <Out()> ByRef blnColumnUpdated As Boolean)
        Dim dblResult As Double
        Dim blnIsNumeric As Boolean

        blnColumnUpdated = False

        If strSplitLine.Length > intColIndex Then
            dblResult = 0
            blnIsNumeric = Double.TryParse(strSplitLine(intColIndex), dblResult)
            If Not blnIsNumeric OrElse dblResult < 1 Then
                strSplitLine(intColIndex) = "1"
                blnColumnUpdated = True
            End If
        End If

    End Sub

End Class
