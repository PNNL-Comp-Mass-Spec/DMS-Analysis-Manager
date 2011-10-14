'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads a tab-delimited text file (created by the Peptide File Extractor or by PHRP)
' and creates a tab-delimited text file suitable for processing by MSGF
' 
' The class must be derived by a sub-class customized for the specific analysis tool (Sequest, X!Tandem, Inspect, etc.)
'
'*********************************************************************************************************

Option Strict On

Public MustInherit Class clsMSGFInputCreator

#Region "Constants"
    Protected Const MSGF_INPUT_FILENAME_SUFFIX As String = "_MSGF_input.txt"
    Public Const MSGF_RESULT_FILENAME_SUFFIX As String = "_MSGF.txt"
#End Region

#Region "Structures"
    Protected Structure udtPHRPDataLine
        Public Title As String
        Public ScanNumber As Integer
        Public Peptide As String                ' Aka annotation
        Public Charge As Short
        Public ProteinFirst As String
        Public ResultID As Integer
        Public PassesFilters As Boolean

        Public Sub Clear()
            Title = String.Empty
            ScanNumber = 0
            Peptide = String.Empty
            Charge = 0
            ProteinFirst = String.Empty
            ResultID = 0
            PassesFilters = False
        End Sub
    End Structure

#End Region

#Region "Module variables"
    Protected mDatasetName As String
    Protected mWorkDir As String
    Protected mDynamicMods As System.Collections.Generic.SortedDictionary(Of String, String)
    Protected mStaticMods As System.Collections.Generic.SortedDictionary(Of String, String)

    ' Column headers in the synopsis file and first hits file
    Protected mColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

    Protected mSkippedLineInfo As System.Collections.Generic.SortedDictionary(Of Integer, System.Collections.Generic.List(Of String))

    Protected mDoNotFilterPeptides As Boolean

    ' This dictionary is initially populated with a string constructed using
    ' Scan plus "_" plus charge plus "_" plus the original peptide sequence in the PHRP file
    ' It will contain an entry for every line written to the MSGF input file
    ' It is later updated by AddUpdateMSGFResult() to store the properly formated MSGF result line for each entry
    ' Finally, it will be used by CreateMSGFFirstHitsFile to create the MSGF file that corresponds to the first-hits file
    Protected mMSGFCachedResults As System.Collections.Generic.SortedDictionary(Of String, String)

    Protected mErrorMessage As String = String.Empty

    Protected mPHRPFirstHitsFilePath As String = String.Empty
    Protected mPHRPSynopsisFilePath As String = String.Empty

    Protected mMSGFInputFilePath As String = String.Empty
    Protected mMSGFResultsFilePath As String = String.Empty

    Protected mMSGFInputFileLineCount As Integer = 0

    Protected mLogFile As System.IO.StreamWriter

#End Region

#Region "Events"
    Public Event ErrorEvent(ByVal strErrorMessage As String)
#End Region

#Region "Properties"

    Public Property DoNotFilterPeptides() As Boolean
        Get
            Return mDoNotFilterPeptides
        End Get
        Set(ByVal value As Boolean)
            mDoNotFilterPeptides = value
        End Set
    End Property

    Public ReadOnly Property MSGFInputFileLineCount() As Integer
        Get
            Return mMSGFInputFileLineCount
        End Get
    End Property

    Public ReadOnly Property MSGFInputFilePath() As String
        Get
            Return mMSGFInputFilePath
        End Get
    End Property

    Public ReadOnly Property MSGFResultsFilePath() As String
        Get
            Return mMSGFResultsFilePath
        End Get
    End Property

    Public ReadOnly Property PHRPFirstHitsFilePath() As String
        Get
            Return mPHRPFirstHitsFilePath
        End Get
    End Property

    Public ReadOnly Property PHRPSynopsisFilePath() As String
        Get
            Return mPHRPSynopsisFilePath
        End Get
    End Property

#End Region

    Public Sub New(ByVal strDatasetName As String, _
                   ByVal strWorkDir As String, _
                   ByRef objDynamicMods As System.Collections.Generic.SortedDictionary(Of String, String), _
                   ByRef objStaticMods As System.Collections.Generic.SortedDictionary(Of String, String))

        mDatasetName = strDatasetName
        mWorkDir = strWorkDir
        mDynamicMods = objDynamicMods
        mStaticMods = objStaticMods

        mErrorMessage = String.Empty

        ' Initialize the column mapping object
        ' Using a case-insensitive comparer
        mColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

        mSkippedLineInfo = New System.Collections.Generic.SortedDictionary(Of Integer, System.Collections.Generic.List(Of String))

        mMSGFCachedResults = New System.Collections.Generic.SortedDictionary(Of String, String)

        ' The following will be overridden by a derived form of this class
        DefineColumnHeaders()

        ' The following will likely be overridden by a derived form of this class
        InitializeFilePaths()

    End Sub

#Region "Functions to be defined in derived classes"
    Protected MustOverride Sub DefineColumnHeaders()
    Protected MustOverride Sub InitializeFilePaths()
    Protected MustOverride Function ParsePHRPDataLine(ByVal intLineNumber As Integer, _
                                                      ByRef strPHRPSource As String, _
                                                      ByRef strColumns() As String, _
                                                      ByRef udtPHRPData As udtPHRPDataLine) As Boolean
#End Region

    ''' <summary>
    ''' Look for dynamic mod symbols in the peptide sequence; replace with the corresponding mod masses
    ''' </summary>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function AddDynamicAndStaticMods(ByVal strPeptide As String, ByRef strPeptideWithMods As String) As Boolean

        Static sbNewPeptide As New System.Text.StringBuilder

        Dim intIndex As Integer
        Dim intIndexStart As Integer
        Dim intIndexEnd As Integer

        Try
            If mDynamicMods.Count = 0 AndAlso mStaticMods.Count = 0 Then
                ' No mods are defined; simply update strPeptideWithMods to be strPeptide
                strPeptideWithMods = strPeptide
                Return True
            End If

            strPeptideWithMods = String.Empty
            sbNewPeptide.Length = 0

            intIndexStart = 0
            intIndexEnd = strPeptide.Length - 1

            If strPeptide.Length >= 4 Then
                If strPeptide.Chars(1) = "." Then
                    ' Peptide is of the form R.HRDTGILDSIGR.F
                    ' Skip the first two characters
                    intIndexStart = 2
                End If

                If strPeptide.Chars(strPeptide.Length - 2) = "." Then
                    ' Peptide is of the form R.HRDTGILDSIGR.F
                    ' Skip the last two characters
                    intIndexEnd = strPeptide.Length - 3
                End If

            End If

            intIndex = 0
            Do While intIndex < strPeptide.Length
                If intIndex < intIndexStart OrElse intIndex > intIndexEnd Then
                    ' We're before or after the primary peptide sequence; simply append the character
                    sbNewPeptide.Append(strPeptide.Chars(intIndex))
                Else
                    If Char.IsLetter(strPeptide.Chars(intIndex)) Then
                        ' Character is a letter; append it
                        sbNewPeptide.Append(strPeptide.Chars(intIndex))

                        ' See if it is present in mStaticMods (this is a case-sensitive search)
                        AddModIfPresent(mStaticMods, strPeptide.Chars(intIndex), sbNewPeptide)

                        If intIndex = intIndexStart AndAlso mStaticMods.Count > 0 Then
                            ' We're at the N-terminus of the peptide
                            ' Possibly add a static N-terminal peptide mod (for example, iTRAQ8, which is 304.2022 DA)
                            AddModIfPresent(mStaticMods, clsMSGFRunner.N_TERMINAL_PEPTIDE_SYMBOL_DMS, sbNewPeptide)

                            If strPeptide.StartsWith(clsMSGFRunner.PROTEIN_TERMINUS_SYMBOL_PHRP) Then
                                ' We're at the N-terminus of the protein
                                ' Possibly add a static N-terminal protein mod
                                AddModIfPresent(mStaticMods, clsMSGFRunner.N_TERMINAL_PROTEIN_SYMBOL_DMS, sbNewPeptide)
                            End If
                        End If
                    Else
                        ' Not a letter; see if it is present in mDynamicMods
                        AddModIfPresent(mDynamicMods, strPeptide.Chars(intIndex), sbNewPeptide)
                    End If

                    If intIndex = intIndexEnd AndAlso mStaticMods.Count > 0 Then
                        ' Possibly add a static C-terminal peptide mod
                        AddModIfPresent(mStaticMods, clsMSGFRunner.C_TERMINAL_PEPTIDE_SYMBOL_DMS, sbNewPeptide)

                        If strPeptide.EndsWith(clsMSGFRunner.PROTEIN_TERMINUS_SYMBOL_PHRP) Then
                            ' We're at the C-terminus of the protein
                            ' Possibly add a static C-terminal protein mod
                            AddModIfPresent(mStaticMods, clsMSGFRunner.C_TERMINAL_PROTEIN_SYMBOL_DMS, sbNewPeptide)
                        End If

                    End If

                End If
                intIndex += 1
            Loop

            strPeptideWithMods = sbNewPeptide.ToString

        Catch ex As Exception
            ReportError("Error adding dynamic and static mods to peptide " & strPeptide & ": " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Protected Sub AddModIfPresent(ByRef objMods As System.Collections.Generic.SortedDictionary(Of String, String), _
                                  ByVal chResidue As Char, _
                                  ByRef sbNewPeptide As System.Text.StringBuilder)

        Dim strModMass As String = String.Empty

        If objMods.TryGetValue(chResidue, strModMass) Then
            ' Static mod applies to this residue; append the mod (add a plus sign if it doesn't start with a minus sign)
            If strModMass.StartsWith("-") Then
                sbNewPeptide.Append(strModMass)
            Else
                sbNewPeptide.Append("+" & strModMass)
            End If
        End If

    End Sub

    Public Sub AddUpdateMSGFResult(ByRef strScanNumber As String, _
                                   ByRef strCharge As String, _
                                   ByRef strPeptide As String, _
                                   ByRef strMSGFResultData As String)

        Try
            mMSGFCachedResults.Item(ConstructMSGFResultCode(strScanNumber, strCharge, strPeptide)) = strMSGFResultData
        Catch ex As Exception
            ' Entry not found; this is unexpected; we will only report the error at the console
            LogError("Entry not found in mMSGFCachedResults for " & ConstructMSGFResultCode(strScanNumber, strCharge, strPeptide))
        End Try

    End Sub

    Public Sub CloseLogFileNow()
        If Not mLogFile Is Nothing Then
            mLogFile.Close()
            mLogFile = Nothing

            GC.Collect()
            GC.WaitForPendingFinalizers()
            System.Threading.Thread.Sleep(100)
        End If
    End Sub

    Protected Function ConstructMSGFResultCode(ByVal intScanNumber As Integer, _
                                               ByVal intCharge As Integer, _
                                               ByRef strPeptide As String) As String

        Return intScanNumber.ToString & "_" & intCharge.ToString & "_" & strPeptide

    End Function

    Protected Function ConstructMSGFResultCode(ByRef strScanNumber As String, _
                                               ByRef strCharge As String, _
                                               ByRef strPeptide As String) As String

        Return strScanNumber & "_" & strCharge & "_" & strPeptide

    End Function

    ''' <summary>
    ''' Read the first-hits file and create a new, parallel file with the MSGF results
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function CreateMSGFFirstHitsFile() As Boolean

        Const MAX_WARNINGS_TO_REPORT As Integer = 10

        Dim srPHRPFile As System.IO.StreamReader
        Dim swMSGFFHTFile As System.IO.StreamWriter

        Dim strMSGFFirstHitsResults As String

        Dim strLineIn As String
        Dim strSplitLine() As String
        Dim strPeptideResultCode As String
        Dim strPHRPSource As String = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_FHT

        Dim strMSGFResultData As String = String.Empty

        Dim intLinesRead As Integer

        Dim intMissingValueCount As Integer
        Dim strWarningMessage As String

        Dim blnSkipLine As Boolean
        Dim blnHeaderLineParsed As Boolean
        Dim blnSuccess As Boolean

        Dim udtPHRPData As udtPHRPDataLine

        Try


            If String.IsNullOrEmpty(mPHRPFirstHitsFilePath) Then
                ' This result type does not have a first-hits file
                Return True
            End If

            ' Open the first-hits file
            srPHRPFile = New System.IO.StreamReader(New System.IO.FileStream(mPHRPFirstHitsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

            ' Define the path to write the first-hits MSGF results to
            strMSGFFirstHitsResults = System.IO.Path.Combine(mWorkDir, _
                                                             System.IO.Path.GetFileNameWithoutExtension(mPHRPFirstHitsFilePath) & MSGF_RESULT_FILENAME_SUFFIX)

            ' Create the output file
            swMSGFFHTFile = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFFirstHitsResults, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

            ' Write out the headers to swMSGFFHTFile
            WriteMSGFResultsHeaders(swMSGFFHTFile)


            intLinesRead = 0
            intMissingValueCount = 0
            blnHeaderLineParsed = False

            Do While srPHRPFile.Peek >= 0
                strLineIn = srPHRPFile.ReadLine
                intLinesRead += 1
                blnSkipLine = False

                If Not String.IsNullOrEmpty(strLineIn) Then
					strSplitLine = strLineIn.Split(ControlChars.Tab)

                    If Not blnHeaderLineParsed Then
                        If Not clsMSGFInputCreator.IsNumber(strSplitLine(0)) Then
                            ' Parse the header line to confirm the column ordering
                            clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, mColumnHeaders)
                            blnSkipLine = True
                        End If

                        blnHeaderLineParsed = True
                    End If

                    If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then

                        blnSuccess = ParsePHRPDataLine(intLinesRead, strPHRPSource, strSplitLine, udtPHRPData)

                        strPeptideResultCode = ConstructMSGFResultCode(udtPHRPData.ScanNumber, udtPHRPData.Charge, udtPHRPData.Peptide)

                        If mMSGFCachedResults.TryGetValue(strPeptideResultCode, strMSGFResultData) Then
                            If String.IsNullOrEmpty(strMSGFResultData) Then
                                ' Match text is empty
                                ' We should not write thie out to disk since it would result in empty columns

                                strWarningMessage = "MSGF Results are empty for result code '" & strPeptideResultCode & "'; this is unexpected"
                                intMissingValueCount += 1
                                If intMissingValueCount <= MAX_WARNINGS_TO_REPORT Then
                                    If intMissingValueCount = MAX_WARNINGS_TO_REPORT Then
                                        strWarningMessage &= "; additional invalid entries will not be reported"
                                    End If
                                    ReportError(strWarningMessage)
                                Else
                                    LogError(strWarningMessage)
                                End If
                            Else
                                ' Match found; write out the result
                                swMSGFFHTFile.WriteLine(udtPHRPData.ResultID & ControlChars.Tab & strMSGFResultData)
                            End If

                        Else
                            ' Match not found; this is unexpected

                            strWarningMessage = "Match not found for first-hits entry with result code '" & strPeptideResultCode & "'; this is unexpected"

                            ' Report the first 10 times this happens
                            intMissingValueCount += 1
                            If intMissingValueCount <= MAX_WARNINGS_TO_REPORT Then
                                If intMissingValueCount = MAX_WARNINGS_TO_REPORT Then
                                    strWarningMessage &= "; additional missing entries will not be reported"
                                End If
                                ReportError(strWarningMessage)
                            Else
                                LogError(strWarningMessage)
                            End If

                        End If

                    End If
                End If

            Loop

            srPHRPFile.Close()
            swMSGFFHTFile.Close()

        Catch ex As Exception
            ReportError("Error creating the MSGF first hits file: " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Creates the input file for MSGF
    ''' Will contain filter passing peptides from the synopsis file, plus all peptides 
    ''' in the first-hits file that are not filter passing in the synopsis file
    ''' If the synopsis file does not exist, then simply processes the first-hits file
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function CreateMSGFInputFileUsingPHRPResultFiles() As Boolean

        Dim srPHRPFile As System.IO.StreamReader
        Dim swMSGFInputFile As System.IO.StreamWriter

        Dim strMzXMLFileName As String = String.Empty
        Dim blnSuccess As Boolean = False

        Try
            If String.IsNullOrEmpty(mDatasetName) Then
                ReportError("Dataset name is undefined; unable to continue")
                Return False
            End If

            If String.IsNullOrEmpty(mWorkDir) Then
                ReportError("Working directory is undefined; unable to continue")
                Return False
            End If

            ' mzXML filename is dataset plus .mzXML
            ' Note that the jrap reader used by MSGF may fail if the .mzXML filename is capitalized differently than this (i.e., it cannot be .mzxml)
            strMzXMLFileName = mDatasetName & ".mzXML"

            ' Create the MSGF Input file that we will write data to
            swMSGFInputFile = New System.IO.StreamWriter(New System.IO.FileStream(mMSGFInputFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

            ' Write out the headers:  #SpectrumFile  Title  Scan#  Annotation  Charge  Protein_First  Result_ID  Data_Source
            ' Note that we're storing the original peptide sequence in the "Title" column, while the marked up sequence (with mod masses) goes in the "Annotation" column
            swMSGFInputFile.WriteLine(clsMSGFRunner.MSGF_RESULT_COLUMN_SpectrumFile & ControlChars.Tab & _
                                      clsMSGFRunner.MSGF_RESULT_COLUMN_Title & ControlChars.Tab & _
                                      clsMSGFRunner.MSGF_RESULT_COLUMN_ScanNumber & ControlChars.Tab & _
                                      clsMSGFRunner.MSGF_RESULT_COLUMN_Annotation & ControlChars.Tab & _
                                      clsMSGFRunner.MSGF_RESULT_COLUMN_Charge & ControlChars.Tab & _
                                      clsMSGFRunner.MSGF_RESULT_COLUMN_Protein_First & ControlChars.Tab & _
                                      clsMSGFRunner.MSGF_RESULT_COLUMN_Result_ID & ControlChars.Tab & _
                                      clsMSGFRunner.MSGF_RESULT_COLUMN_Data_Source)

            ' Initialize some tracking variables
            mMSGFInputFileLineCount = 1

            mSkippedLineInfo.Clear()

            mMSGFCachedResults.Clear()


            If Not String.IsNullOrEmpty(mPHRPSynopsisFilePath) AndAlso System.IO.File.Exists(mPHRPSynopsisFilePath) Then
                ' Read the synopsis file data
                srPHRPFile = New System.IO.StreamReader(New System.IO.FileStream(mPHRPSynopsisFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

                ReadAndStorePHRPData(srPHRPFile, swMSGFInputFile, strMzXMLFileName, True)
                blnSuccess = True
            End If


            If Not String.IsNullOrEmpty(mPHRPFirstHitsFilePath) AndAlso System.IO.File.Exists(mPHRPFirstHitsFilePath) Then
                ' Now read the first-hits file data
                srPHRPFile = New System.IO.StreamReader(New System.IO.FileStream(mPHRPFirstHitsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

                ReadAndStorePHRPData(srPHRPFile, swMSGFInputFile, strMzXMLFileName, False)
                blnSuccess = True
            End If

            swMSGFInputFile.Close()

            If Not blnSuccess Then
                ReportError("Neither the _syn.txt nor the _fht.txt file was found")
            End If

        Catch ex As Exception
            ReportError("Error reading the PHRP result file to create the MSGF Input file: " & ex.Message)

            If Not srPHRPFile Is Nothing Then srPHRPFile.Close()
            If Not swMSGFInputFile Is Nothing Then swMSGFInputFile.Close()

            Return False
        End Try

        Return blnSuccess

    End Function

    Public Function GetSkippedInfoByResultId(ByVal intResultID As Integer) As System.Collections.Generic.List(Of String)

        Dim objSkipList As System.Collections.Generic.List(Of String)

        If mSkippedLineInfo.TryGetValue(intResultID, objSkipList) Then
            Return objSkipList
        Else
            Return New System.Collections.Generic.List(Of String)()
        End If

    End Function

    Public Shared Function IsNumber(ByVal strData As String) As Boolean

        If Double.TryParse(strData, 0) Then
            Return True
        ElseIf Integer.TryParse(strData, 0) Then
            Return True
        End If

        Return False

    End Function

    Protected Sub LogError(ByVal strErrorMessage As String)

        Try
            If mLogFile Is Nothing Then
                Dim strErrorLogFilePath As String
                Dim blnWriteHeader As Boolean = True

                strErrorLogFilePath = System.IO.Path.Combine(mWorkDir, "MSGFInputCreator_Log.txt")

                If System.IO.File.Exists(strErrorLogFilePath) Then
                    blnWriteHeader = False
                End If

                mLogFile = New System.IO.StreamWriter(New System.IO.FileStream(strErrorLogFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.ReadWrite))
                mLogFile.AutoFlush = True

                If blnWriteHeader Then
                    mLogFile.WriteLine("Date" & ControlChars.Tab & "Message")
                End If
            End If

            mLogFile.WriteLine(System.DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") & ControlChars.Tab & strErrorMessage)

        Catch ex As Exception
            RaiseEvent ErrorEvent("Error writing to MSGFInputCreator log file: " & ex.Message)
        End Try

    End Sub

    ''' <summary>
    ''' Returns the string stored in the given named column (using objColumnHeaders to dereference column name with column index)
    ''' </summary>
    ''' <returns>The text in the specified column; an empty string if the specific column name is not recognized</returns>
    ''' <remarks></remarks>
    Public Shared Function LookupColumnValue(ByRef strColumns() As String, _
                                             ByVal strColumnName As String, _
                                             ByRef objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)) As String

        Return LookupColumnValue(strColumns, strColumnName, objColumnHeaders, String.Empty)
    End Function

    ''' <summary>
    ''' Returns the string stored in the given named column (using objColumnHeaders to dereference column name with column index)
    ''' </summary>
    ''' <returns>The text in the specified column; strValueIfMissing if the specific column name is not recognized</returns>
    ''' <remarks></remarks>
    Public Shared Function LookupColumnValue(ByRef strColumns() As String, _
                                             ByVal strColumnName As String, _
                                             ByRef objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer), _
                                             ByVal strValueIfMissing As String) As String

        Dim intColIndex As Integer

        If Not strColumns Is Nothing Then
            If objColumnHeaders.TryGetValue(strColumnName, intColIndex) Then
                If intColIndex >= 0 AndAlso intColIndex < strColumns.Length Then
                    If String.IsNullOrWhiteSpace(strColumns(intColIndex)) Then
                        Return String.Empty
                    Else
                        Return strColumns(intColIndex)
                    End If
                End If
            End If
        End If

        ' If we get here, return strValueIfMissing
        Return strValueIfMissing

    End Function

    ''' <summary>
    ''' Returns the value stored in the given named column (using objColumnHeaders to dereference column name with column index)
    ''' </summary>
    ''' <returns>The number in the specified column; 0 if the specific column name is not recognized or the column does not contain a number</returns>
    ''' <remarks></remarks>
    Public Shared Function LookupColumnValue(ByRef strColumns() As String, _
                                             ByVal strColumnName As String, _
                                             ByRef objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer), _
                                             ByVal ValueIfMissing As Integer) As Integer

        Dim strValue As String
        Dim intValue As Integer

        strValue = LookupColumnValue(strColumns, strColumnName, objColumnHeaders, ValueIfMissing.ToString)

        Integer.TryParse(strValue, intValue)

        Return intValue

    End Function

    ''' <summary>
    ''' Returns the value stored in the given named column (using objColumnHeaders to dereference column name with column index)
    ''' </summary>
    ''' <returns>The number in the specified column; 0 if the specific column name is not recognized or the column does not contain a number</returns>
    ''' <remarks></remarks>
    Public Shared Function LookupColumnValue(ByRef strColumns() As String, _
                                             ByVal strColumnName As String, _
                                             ByRef objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer), _
                                             ByVal ValueIfMissing As Double) As Double

        Dim strValue As String
        Dim dblValue As Double

        strValue = LookupColumnValue(strColumns, strColumnName, objColumnHeaders, ValueIfMissing.ToString)

        Double.TryParse(strValue, dblValue)

        Return dblValue

    End Function

    ''' <summary>
    ''' Updates the column name to column index mapping in objColumnHeaders
    ''' </summary>
    ''' <param name="strColumns">Column names read from the input file</param>
    ''' <param name="objColumnHeaders">Column mapping dictionary object to update</param>
    ''' <remarks>The SortedDictionary object should be instantiated using a case-insensitive comparer, i.e. (StringComparer.CurrentCultureIgnoreCase)</remarks>
    Public Shared Sub ParseColumnHeaders(ByVal strColumns() As String, _
                                         ByRef objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer))

        Dim intIndex As Integer

        For intIndex = 0 To strColumns.Length - 1
            If objColumnHeaders.ContainsKey(strColumns(intIndex)) Then
                ' Update the index associated with this column name
                objColumnHeaders(strColumns(intIndex)) = intIndex
            Else
                ' Ignore this column
            End If
        Next intIndex

    End Sub

    ''' <summary>
    ''' Read data from a synopsis file or first hits file
    ''' Write filter-passing synopsis file data to the MSGF input file
    ''' Write first-hits data to the MSGF input file only if it isn't in mMSGFCachedResults
    ''' </summary>
    ''' <param name="srPHRPFile"></param>
    ''' <param name="swMSGFInputFile"></param>
    ''' <param name="strMzXMLFileName"></param>
    ''' <param name="blnParsingSynopsisFile"></param>
    ''' <remarks></remarks>
    Private Sub ReadAndStorePHRPData(ByRef srPHRPFile As System.IO.StreamReader, _
                                     ByRef swMSGFInputFile As System.IO.StreamWriter, _
                                     ByVal strMzXMLFileName As String, _
                                     ByVal blnParsingSynopsisFile As Boolean)

        Dim strPeptideWithMods As String = String.Empty

        Dim strLineIn As String
        Dim strSplitLine() As String
        Dim strPeptideResultCode As String
        Dim strPHRPSource As String

        Dim intLinesRead As Integer

        Dim blnSkipLine As Boolean
        Dim blnHeaderLineParsed As Boolean
        Dim blnSuccess As Boolean

        Dim udtPHRPDataPrevious As udtPHRPDataLine
        Dim udtPHRPData As udtPHRPDataLine

        Dim objSkipList As System.Collections.Generic.List(Of String)

        If blnParsingSynopsisFile Then
            strPHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_SYN
        Else
            strPHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_FHT
        End If

        udtPHRPDataPrevious.Clear()
        udtPHRPData.Clear()

        intLinesRead = 0
        blnHeaderLineParsed = False

        Do While srPHRPFile.Peek >= 0
            strLineIn = srPHRPFile.ReadLine
            intLinesRead += 1
            blnSkipLine = False

            If Not String.IsNullOrEmpty(strLineIn) Then
                strSplitLine = strLineIn.Split(ControlChars.Tab)

                If Not blnHeaderLineParsed Then
                    If Not clsMSGFInputCreator.IsNumber(strSplitLine(0)) Then
                        ' Parse the header line to confirm the column ordering
                        clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, mColumnHeaders)
                        blnSkipLine = True
                    End If

                    blnHeaderLineParsed = True
                End If

                If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then

                    blnSuccess = ParsePHRPDataLine(intLinesRead, strPHRPSource, strSplitLine, udtPHRPData)

                    ' Compute the result code; we'll use it later to search/populate mMSGFCachedResults
                    strPeptideResultCode = ConstructMSGFResultCode(udtPHRPData.ScanNumber, udtPHRPData.Charge, udtPHRPData.Peptide)

                    If mDoNotFilterPeptides Then
                        udtPHRPData.PassesFilters = True
                    End If

                    If blnParsingSynopsisFile Then
                        ' Synopsis file 
                        ' Check for duplicate lines

                        If blnSuccess And udtPHRPData.PassesFilters Then
                            ' If this line is a duplicate of the previous line, then skip it
                            ' This happens in Sequest _syn.txt files where the line is repeated for all protein matches
                            With udtPHRPDataPrevious
                                If .ScanNumber = udtPHRPData.ScanNumber AndAlso _
                                   .Charge = udtPHRPData.Charge AndAlso _
                                   .Peptide = udtPHRPData.Peptide Then

                                    blnSuccess = False

                                    If mSkippedLineInfo.TryGetValue(.ResultID, objSkipList) Then
                                        objSkipList.Add(udtPHRPData.ResultID & ControlChars.Tab & udtPHRPData.ProteinFirst)
                                    Else
                                        objSkipList = New System.Collections.Generic.List(Of String)
                                        objSkipList.Add(udtPHRPData.ResultID & ControlChars.Tab & udtPHRPData.ProteinFirst)
                                        mSkippedLineInfo.Add(.ResultID, objSkipList)
                                    End If
                                Else
                                    ' Update udtPHRPDataPrevious
                                    ' Since this is a structure, "=" will result in a member-by-member copy
                                    udtPHRPDataPrevious = udtPHRPData
                                End If
                            End With
                        End If

                    Else
                        ' First-hits file
                        ' Override PassesFilters, but see if this entry is present in mMSGFCachedResults

                        udtPHRPData.PassesFilters = True

                        If mMSGFCachedResults.ContainsKey(strPeptideResultCode) Then
                            blnSuccess = False
                        End If

                    End If

                    If blnSuccess And udtPHRPData.PassesFilters Then
                        ' Markup the peptide with the dynamic and static mods
                        blnSuccess = AddDynamicAndStaticMods(udtPHRPData.Peptide.Trim, strPeptideWithMods)
                    End If

                    If blnSuccess And udtPHRPData.PassesFilters Then
                        With udtPHRPData

                            ' The title column holds the original peptide sequence
                            ' If a peptide doesn't have any mods, then the Title column and the Annotation column will be identical

                            swMSGFInputFile.WriteLine(strMzXMLFileName & ControlChars.Tab & _
                                                      .Peptide & ControlChars.Tab & _
                                                      .ScanNumber & ControlChars.Tab & _
                                                      strPeptideWithMods & ControlChars.Tab & _
                                                      .Charge & ControlChars.Tab & _
                                                      .ProteinFirst & ControlChars.Tab & _
                                                      .ResultID & ControlChars.Tab & _
                                                      strPHRPSource)

                            mMSGFInputFileLineCount += 1

                            Try
                                mMSGFCachedResults.Add(strPeptideResultCode, "")
                            Catch ex As Exception
                                ' Key is already present; this is unexpected, but we can safely ignore this error
                                LogError("Warning in ReadAndStorePHRPData: Key already defined in mMSGFCachedResults: " & strPeptideResultCode)
                            End Try


                        End With
                    End If

                End If
            End If

        Loop

        srPHRPFile.Close()
    End Sub

    Protected Sub ReportError(ByVal strErrorMessage As String)
        mErrorMessage = strErrorMessage
        LogError(mErrorMessage)
        RaiseEvent ErrorEvent(mErrorMessage)
    End Sub

    ''' <summary>
    ''' Define the MSGF input and output file paths
    ''' </summary>
    ''' <remarks>This sub should be called after updating mPHRPResultFilePath</remarks>
    Protected Sub UpdateMSGFInputOutputFilePaths()
        mMSGFInputFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mPHRPSynopsisFilePath) & MSGF_INPUT_FILENAME_SUFFIX)
        mMSGFResultsFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mPHRPSynopsisFilePath) & MSGF_RESULT_FILENAME_SUFFIX)
    End Sub

    Public Sub WriteMSGFResultsHeaders(ByRef swOutFile As System.IO.StreamWriter)

        swOutFile.WriteLine("Result_ID" & ControlChars.Tab & _
                            "Scan" & ControlChars.Tab & _
                            "Charge" & ControlChars.Tab & _
                            "Protein" & ControlChars.Tab & _
                            "Peptide" & ControlChars.Tab & _
                            "SpecProb" & ControlChars.Tab & _
                            "Notes")
    End Sub

End Class
