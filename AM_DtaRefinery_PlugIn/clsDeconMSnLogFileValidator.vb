Option Strict On

''' <summary>
''' This class can be used to validate the data in a _DeconMSn_log.txt file
''' It makes sure that the intensity values in the last two columns are all > 0
''' Values that are 0 are auto-changed to 1
''' </summary>
''' <remarks></remarks>
Public Class clsDeconMSnLogFileValidator

    Protected mErrorMessage As String = String.Empty
    Protected mFileUpdated As Boolean

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

    Private Function CollapseLine(ByRef strSplitLine() As String) As String
        Dim sbCollapsed As New System.Text.StringBuilder(1024)

        If strSplitLine.Length > 0 Then
            sbCollapsed.Append(strSplitLine(0))
            For intIndex As Integer = 1 To strSplitLine.Length - 1
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
    Public Function ValidateDeconMSnLogFile(ByVal strSourceFilePath As String) As Boolean

        Dim srSourceFile As System.IO.StreamReader

        Dim strTempFilePath As String
        Dim swOutFile As System.IO.StreamWriter

        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim blnHeaderPassed As Boolean = False
        Dim blnColumnUpdated As Boolean

        Dim intParentIntensityColIndex As Integer = 9
        Dim intMonoIntensityColIndex As Integer = 10
        Dim intColumnCountUpdated As Integer

        Try

            mErrorMessage = String.Empty
            mFileUpdated = False

            srSourceFile = New System.IO.StreamReader(New System.IO.FileStream(strSourceFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

            strTempFilePath = System.IO.Path.GetTempFileName()
            System.Threading.Thread.Sleep(250)

            swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strTempFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.ReadWrite))

            Do While srSourceFile.Peek > -1
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

                Else
                    If strLineIn.StartsWith("--------------") Then
                        blnHeaderPassed = True
                    ElseIf strLineIn.StartsWith("MSn_Scan") Then
                        ValidateHeader(strLineIn, intParentIntensityColIndex, intMonoIntensityColIndex)
                        blnHeaderPassed = True
                    End If
                End If

                If intColumnCountUpdated > 0 Then
                    mFileUpdated = True
                    swOutFile.WriteLine(CollapseLine(strSplitLine))
                Else
                    swOutFile.WriteLine(strLineIn)
                End If

            Loop

            srSourceFile.Close()
            swOutFile.Close()

            If mFileUpdated Then
                ' First rename strFilePath
                Dim ioFileInfo As System.IO.FileInfo = New System.IO.FileInfo(strSourceFilePath)
                Dim strTargetFilePath As String = System.IO.Path.Combine(ioFileInfo.DirectoryName, System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.Name) & "_Original.txt")

                If System.IO.File.Exists(strTargetFilePath) Then
                    Try
                        System.IO.File.Delete(strTargetFilePath)
                    Catch ex As Exception
                        mErrorMessage = "Error deleting old _Original.txt file: " & ex.Message
                        Console.WriteLine(mErrorMessage)
                    End Try
                End If

                Try
                    ioFileInfo.MoveTo(strTargetFilePath)

                    ' Now copy the temp file to strFilePath
                    System.IO.File.Copy(strTempFilePath, strSourceFilePath, False)

                Catch ex As Exception
                    mErrorMessage = "Error replacing source file with new file: " & ex.Message
                    Console.WriteLine(mErrorMessage)

                    ' Copy the temp file to strFilePath
                    System.IO.File.Copy(strTempFilePath, System.IO.Path.Combine(ioFileInfo.DirectoryName, System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.Name) & "_New.txt"), True)
                    System.IO.File.Delete(strTempFilePath)

                    Return False
                End Try

            End If

            System.IO.File.Delete(strTempFilePath)

        Catch ex As Exception
            mErrorMessage = "Exception in clsDeconMSnLogFileValidator.ValidateFile: " & ex.Message
            Console.WriteLine(mErrorMessage)
            Return False
        End Try

        Return True

    End Function

    Private Sub ValidateHeader(ByVal strLineIn As String, ByRef intParentIntensityColIndex As Integer, ByRef intMonoIntensityColIndex As Integer)
        Dim strSplitLine() As String
        Dim lstSplitLine As System.Collections.Generic.List(Of String)
        Dim intColIndex As Integer

        strSplitLine = strLineIn.Split(ControlChars.Tab)

        If strSplitLine.Length > 1 Then
            lstSplitLine = New System.Collections.Generic.List(Of String)(strSplitLine)

            intColIndex = lstSplitLine.IndexOf("Parent_Intensity")
            If intColIndex > 0 Then intParentIntensityColIndex = intColIndex

            intColIndex = lstSplitLine.IndexOf("Mono_Intensity")
            If intColIndex > 0 Then intMonoIntensityColIndex = intColIndex
        End If

    End Sub

    Private Sub ValidateColumnIsPositive(ByRef strSplitLine() As String, ByVal intColIndex As Integer, ByRef blnColumnUpdated As Boolean)
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
