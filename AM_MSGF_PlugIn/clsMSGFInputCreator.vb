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
    Protected Const MSGF_RESULT_FILENAME_SUFFIX As String = "_MSGF.txt"
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

    Protected mColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

    Protected mErrorMessage As String = String.Empty

    Protected mPHRPResultFilePath As String = String.Empty
    Protected mMSGFInputFilePath As String = String.Empty
    Protected mMSGFResultsFilePath As String = String.Empty

    Protected mMSGFInputFileLineCount As Integer = 0

#End Region

#Region "Events"
    Public Event ErrorEvent(ByVal strErrorMessage As String)
#End Region

#Region "Properties"
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

    Public ReadOnly Property PHRPResultFilePath() As String
        Get
            Return mPHRPResultFilePath
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

        ' The following will be overridden by a derived form of this class
        DefineColumnHeaders()

        ' The following will likely be overridden by a derived form of this class
        InitializeFilePaths()

    End Sub

#Region "Functions to be defined in derived classes"
    Protected MustOverride Sub DefineColumnHeaders()
    Protected MustOverride Sub InitializeFilePaths()
    Protected MustOverride Function ParsePHRPDataLine(ByVal intLineNumber As Integer, ByRef strColumns() As String, ByRef udtPHRPData As udtPHRPDataLine) As Boolean
#End Region

    ''' <summary>
    ''' Look for dynamic mod symbols in the peptide sequence; replace with the corresponding mod masses
    ''' </summary>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function AddDynamicAndStaticMods(ByVal strPeptide As String, ByRef strPeptideWithMods As String) As Boolean

        Static sbNewPeptide As New System.Text.StringBuilder

        'Dim objEnum As System.Collections.Generic.SortedDictionary(Of String, String).Enumerator
        Dim intIndex As Integer
        Dim intIndexStart As Integer
        Dim intIndexEnd As Integer

        Dim strModMass As String = String.Empty

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
                        If mStaticMods.TryGetValue(strPeptide.Chars(intIndex), strModMass) Then
                            ' Static mod applies to this residue; append the mod (add a plus sign if it doesn't start with a minus sign)
                            If strModMass.StartsWith("-") Then
                                sbNewPeptide.Append(strModMass)
                            Else
                                sbNewPeptide.Append("+" & strModMass)
                            End If

                        End If
                    Else
                        ' Not a letter; see if it is present in mDynamicMods
                        If mDynamicMods.TryGetValue(strPeptide.Chars(intIndex), strModMass) Then
                            ' Dynamic mod applies to the previous residue; append the mod (add a plus sign if it doesn't start with a minus sign)
                            If strModMass.StartsWith("-") Then
                                sbNewPeptide.Append(strModMass)
                            Else
                                sbNewPeptide.Append("+" & strModMass)
                            End If
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

    Public Function CreateMSGFInputFileUsingPHRPResultFile() As Boolean

        Dim srPHRPFile As System.IO.StreamReader
        Dim swMSGFInputFile As System.IO.StreamWriter

        Dim strMzXMLFileName As String = String.Empty
        Dim strPeptideWithMods As String = String.Empty
        Dim strTitle As String

        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim intLinesRead As Integer

        Dim blnSkipLine As Boolean
        Dim blnHeaderLineParsed As Boolean
        Dim blnSuccess As Boolean

        Dim udtPHRPDataPrevious As udtPHRPDataLine
        Dim udtPHRPData As udtPHRPDataLine

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
            ' Note that the jrap reader used by will fail if the .mzXML filename is capitalized differently than this (i.e., it cannot be .mzxml)
            strMzXMLFileName = mDatasetName & ".mzXML"

            ' Open the PHRP result file
            srPHRPFile = New System.IO.StreamReader(New System.IO.FileStream(mPHRPResultFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

            ' Create the MSGF Input file that we will write data to
            swMSGFInputFile = New System.IO.StreamWriter(New System.IO.FileStream(mMSGFInputFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

            ' Write out the headers
            ' Note that if the peptide has any dynamic or static mods, then we're storing the original peptide sequence in the "Title" column, while the marked up sequence (with mod masses) goes in the "Annotation" column
            swMSGFInputFile.WriteLine("#SpectrumFile" & ControlChars.Tab & _
                                      "Title" & ControlChars.Tab & _
                                      "Scan#" & ControlChars.Tab & _
                                      "Annotation" & ControlChars.Tab & _
                                      "Charge" & ControlChars.Tab & _
                                      "Protein_First" & ControlChars.Tab & _
                                      "Result_ID")

            blnHeaderLineParsed = False
            intLinesRead = 0
            mMSGFInputFileLineCount = 1

            udtPHRPDataPrevious.Clear()
            udtPHRPData.Clear()

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

                        blnSuccess = ParsePHRPDataLine(intLinesRead, strSplitLine, udtPHRPData)

                        If blnSuccess And udtPHRPData.PassesFilters Then
                            ' If this line is a duplicate of the previous line, then skip it
                            ' This happens in Sequest _syn.txt files where the line is repeated for all protein matches
                            With udtPHRPDataPrevious
                                If .ScanNumber = udtPHRPData.ScanNumber AndAlso _
                                   .Charge = udtPHRPData.Charge AndAlso _
                                   .Peptide = udtPHRPData.Peptide Then

                                    blnSuccess = False
                                End If
                            End With
                        End If

                        If blnSuccess And udtPHRPData.PassesFilters Then
                            ' Markup the peptide with the dynamic and static mods
                            blnSuccess = AddDynamicAndStaticMods(udtPHRPData.Peptide.Trim, strPeptideWithMods)
                        End If

                        If blnSuccess And udtPHRPData.PassesFilters Then
                            With udtPHRPData

                                If udtPHRPData.Peptide = strPeptideWithMods Then
                                    ' No mods
                                    strTitle = ""
                                Else
                                    strTitle = udtPHRPData.Peptide
                                End If

                                swMSGFInputFile.WriteLine(strMzXMLFileName & ControlChars.Tab & _
                                                          strTitle & ControlChars.Tab & _
                                                          .ScanNumber & ControlChars.Tab & _
                                                          strPeptideWithMods & ControlChars.Tab & _
                                                          .Charge & ControlChars.Tab & _
                                                          .ProteinFirst & ControlChars.Tab & _
                                                          .ResultID)

                                mMSGFInputFileLineCount += 1
                            End With
                        End If

                        ' Update udtPHRPDataPrevious
                        ' Since this is a structure, "=" will result in a member-by-member copy
                        udtPHRPDataPrevious = udtPHRPData

                    End If
                End If

            Loop

            srPHRPFile.Close()
            swMSGFInputFile.Close()

        Catch ex As Exception
            ReportError("Error reading the PHRP result file to create the MSGF Input file: " & ex.Message)

            If Not srPHRPFile Is Nothing Then srPHRPFile.Close()
            If Not swMSGFInputFile Is Nothing Then swMSGFInputFile.Close()

            Return False
        End Try

        Return True

    End Function

    Public Shared Function IsNumber(ByVal strData As String) As Boolean

        If Double.TryParse(strData, 0) Then
            Return True
        ElseIf Integer.TryParse(strData, 0) Then
            Return True
        End If

        Return False

    End Function

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
                If intColIndex < strColumns.Length Then
                    If strColumns(intColIndex) Is Nothing Then
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

    Protected Sub ReportError(ByVal strErrorMessage As String)
        mErrorMessage = strErrorMessage

        RaiseEvent ErrorEvent(mErrorMessage)
    End Sub

    ''' <summary>
    ''' Define the MSGF input and output file paths
    ''' </summary>
    ''' <remarks>This sub should be called after updating mPHRPResultFilePath</remarks>
    Protected Sub UpdateMSGFInputOutputFilePaths()
        mMSGFInputFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mPHRPResultFilePath) & MSGF_INPUT_FILENAME_SUFFIX)
        mMSGFResultsFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mPHRPResultFilePath) & MSGF_RESULT_FILENAME_SUFFIX)
    End Sub

End Class
