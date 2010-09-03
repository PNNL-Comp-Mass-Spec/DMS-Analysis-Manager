'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsMSGFRunner
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Primary class for running MSGF
	'*********************************************************************************************************

#Region "Constants and enums"

    Protected Const PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD As Single = 2     
    Protected Const PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED As Single = 3
    Protected Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 6
    Protected Const PROGRESS_PCT_MZXML_CREATED As Single = 10
    Protected Const PROGRESS_PCT_MSGF_START As Single = PROGRESS_PCT_MZXML_CREATED
    Protected Const PROGRESS_PCT_MSGF_COMPLETE As Single = 95
    Protected Const PROGRESS_PCT_MSGF_POST_PROCESSING As Single = 97

    Public Const N_TERMINAL_PEPTIDE_SYMBOL_DMS As Char = "<"c
    Public Const C_TERMINAL_PEPTIDE_SYMBOL_DMS As Char = ">"c
    Public Const N_TERMINAL_PROTEIN_SYMBOL_DMS As Char = "["c
    Public Const C_TERMINAL_PROTEIN_SYMBOL_DMS As Char = "]"c
    Public Const PROTEIN_TERMINUS_SYMBOL_PHRP As Char = "-"c

    Protected Const MOD_SUMMARY_COLUMN_Modification_Symbol As String = "Modification_Symbol"
    Protected Const MOD_SUMMARY_COLUMN_Modification_Mass As String = "Modification_Mass"
    Protected Const MOD_SUMMARY_COLUMN_Target_Residues As String = "Target_Residues"
    Protected Const MOD_SUMMARY_COLUMN_Modification_Type As String = "Modification_Type"
    Protected Const MOD_SUMMARY_COLUMN_Mass_Correction_Tag As String = "Mass_Correction_Tag"
    Protected Const MOD_SUMMARY_COLUMN_Occurence_Count As String = "Occurence_Count"

    Protected Const XT_SEQ_PROT_MAP_COLUMN_Unique_Seq_ID As String = "Unique_Seq_ID"
    Protected Const XT_SEQ_PROT_MAP_COLUMN_Cleavage_State As String = "Cleavage_State"
    Protected Const XT_SEQ_PROT_MAP_COLUMN_Terminus_State As String = "Terminus_State"
    Protected Const XT_SEQ_PROT_MAP_COLUMN_Protein_Name As String = "Protein_Name"
    Protected Const XT_SEQ_PROT_MAP_COLUMN_Protein_EValue As String = "Protein_Expectation_Value_Log(e)"
    Protected Const XT_SEQ_PROT_MAP_COLUMN_Protein_Intensity As String = "Protein_Intensity_Log(I)"

    Public Const MSGF_RESULT_COLUMN_SpectrumFile As String = "#SpectrumFile"
    Public Const MSGF_RESULT_COLUMN_Title As String = "Title"
    Public Const MSGF_RESULT_COLUMN_ScanNumber As String = "Scan#"
    Public Const MSGF_RESULT_COLUMN_Annotation As String = "Annotation"
    Public Const MSGF_RESULT_COLUMN_Charge As String = "Charge"
    Public Const MSGF_RESULT_COLUMN_Protein_First As String = "Protein_First"
    Public Const MSGF_RESULT_COLUMN_Result_ID As String = "Result_ID"
    Public Const MSGF_RESULT_COLUMN_SpecProb As String = "SpecProb"
    Public Const MSGF_RESULT_COLUMN_Data_Source As String = "Data_Source"

    Public Const MSGF_PHRP_DATA_SOURCE_SYN As String = "Syn"
    Public Const MSGF_PHRP_DATA_SOURCE_FHT As String = "FHT"

    Public Const XT_RESULT_TO_SEQ_MAP_SUFFIX As String = "_xt_ResultToSeqMap.txt"
    Public Const XT_SEQ_TO_PROTEIN_MAP_SUFFIX As String = "_xt_SeqToProteinMap.txt"

    Public Enum ePeptideHitResultType
        Unknown = 0
        Sequest = 1
        XTandem = 2
        Inspect = 3
    End Enum

#End Region

#Region "Module variables"
    Protected m_DatasetName As String = String.Empty
    Protected m_ETDMode As Boolean = False

    Protected mMSGFInputFilePath As String = String.Empty
    Protected mMSGFResultsFilePath As String = String.Empty
    Protected mMSGFInputFileLineCount As Integer = 0

    Protected WithEvents mMSXmlGenReadW As clsMSXMLGenReadW
    Protected WithEvents mMSGFInputCreator As clsMSGFInputCreator
    Protected WithEvents mMSGFRunner As clsRunDosProgram

#End Region

#Region "Events"
#End Region

#Region "Properties"
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs MSGF
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType representing success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim eResultType As ePeptideHitResultType
        Dim Msg As String = String.Empty

        Dim blnSuccess As Boolean
        Dim Result As IJobParams.CloseOutType
        Dim eReturnCode As IJobParams.CloseOutType

        Dim blnProcessingError As Boolean
        Dim blnUseExistingMSGFResults As Boolean
        Dim blnPostProcessingError As Boolean

        ' Set this to success for now
        eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        'Call base class for initial setup
        MyBase.RunTool()

        ' Cache the dataset name
        m_DatasetName = m_jobParams.GetParam("datasetNum")

        ' Resolve eResultType
        eResultType = GetPeptideHitResultType(m_jobParams.GetParam("ResultType"))

        If eResultType = ePeptideHitResultType.Unknown Then
            ' Result type is not supported

            Msg = "ResultType is not supported by MSGF: " & m_jobParams.GetParam("ResultType")
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMSGFToolRunner.RunTool(); " & Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Try
            ' Make sure clsGlobal.m_Completions_Msg is empty
            clsGlobal.m_Completions_Msg = String.Empty
            blnProcessingError = False


            ' Parse the Sequest, X!Tandem, or Inspect parameter file to determine if ETD mode was used
            Dim strParamFilePath As String
            strParamFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName"))

            blnSuccess = CheckETDModeEnabled(eResultType, strParamFilePath)
            If Not blnSuccess Then
                Msg = "Error examining param file to determine if ETD mode was enabled)"
                m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMSGFToolRunner.RunTool(); " & Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            Else
                m_progress = PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD
                m_StatusTools.UpdateAndWrite(m_progress)
            End If

            ' Create the _MSGF_input.txt file
            blnSuccess = CreateMSGFInputFile(eResultType)

            If Not blnSuccess Then
                Msg = "Error creating MSGF input file"
                m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                blnProcessingError = True
            Else
                m_progress = PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED
                m_StatusTools.UpdateAndWrite(m_progress)
            End If


            If Not blnProcessingError Then
                ' Create the .mzXML file
                ' We're waiting to do this until now just in case the above steps fail (since they should all run quickly)
                blnSuccess = CreateMZXMLFile()

                If Not blnSuccess Then
                    Msg = "Error creating .mzXML file"
                    m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                    blnProcessingError = True
                Else
                    m_progress = PROGRESS_PCT_MZXML_CREATED
                    m_StatusTools.UpdateAndWrite(m_progress)
                End If
            End If


            If Not blnProcessingError Then
                blnUseExistingMSGFResults = clsGlobal.GetJobParameter(m_jobParams, "UseExistingMSGFResults", False)

                If blnUseExistingMSGFResults Then
                    ' Look for a file named Dataset_syn_MSGF.txt in the job's transfer folder
                    ' If that file exists, use it as the official MSGF results file
                    ' The assumption is that this file will have been created by manually running MSGF on another computer

                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "UseExistingMSGFResults = True; will look for pre-generated MSGF results file in the transfer folder")
                    End If

                    If RetrievePreGeneratedDataFile(System.IO.Path.GetFileName(mMSGFResultsFilePath)) Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Pre-generated MSGF results file successfully copied to the work directory")
                        blnSuccess = True
                    Else
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Pre-generated MSGF results file not found")
                        blnSuccess = False
                    End If

                Else
                    ' Run MSGF
                    blnSuccess = RunMSGF()
                End If

                If Not blnSuccess Then
                    Msg = "Error running MSGF"
                    m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                    blnProcessingError = True
                Else
                    ' MSGF successfully completed
                    If Not clsGlobal.GetJobParameter(m_jobParams, "KeepMSGFInputFile", False) Then
                        ' Add the _MSGF_input.txt file to the list of files to delete (i.e., do not move it into the results folder)
                        clsGlobal.FilesToDelete.Add(System.IO.Path.GetFileName(mMSGFInputFilePath))
                    End If

                    m_progress = PROGRESS_PCT_MSGF_COMPLETE
                    m_StatusTools.UpdateAndWrite(m_progress)
                End If
            End If

            If Not blnProcessingError Then
                ' Post-process the MSGF output file to create two new MSGF result files, one for the synopsis file and one for the first-hits file
                ' Will also make sure that all of the peptides have numeric SpecProb values
                ' For peptides where MSGF reported an error, the MSGF SpecProb will be set to 1

                ' Sleep for 1 second to give the MSGF results file a chance to finalize
                System.Threading.Thread.Sleep(1000)

                blnSuccess = PostProcessMSGFResults(eResultType, mMSGFResultsFilePath)

                If Not blnSuccess Then
                    Msg = "MSGF results file post-processing error"
                    m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                    blnPostProcessingError = True
                End If
            End If

            'Stop the job timer
            m_StopTime = Now

            If blnProcessingError Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            System.Threading.Thread.Sleep(2000)        '2 second delay
            GC.Collect()
            GC.WaitForPendingFinalizers()

            Result = MakeResultsFolder()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Result = MoveResultFiles()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MoveResultFiles moves the result files to the result folder
                m_message = "Error moving files into results folder"
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If blnProcessingError Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
                ' Try to save whatever files were moved into the results folder
                Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
                objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Result = CopyResultsFolderToServer()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return Result
            End If

            If blnPostProcessingError Then
                ' When a post-processing error occurs, we copy the files to the server, but return CLOSEOUT_FAILED
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As System.Exception
            Msg = "clsMSGFToolRunner.RunTool(); Exception running MSGF: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception running MSGF")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'If we get to here, everything worked so exit happily
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Examines the Sequest, X!Tandem, or Inspect param file to determine if ETD mode is enabled
    ''' </summary>
    ''' <param name="eResultType"></param>
    ''' <param name="strParamFilePath"></param>
    ''' <returns>True if success; false if an error</returns>
    Protected Function CheckETDModeEnabled(ByVal eResultType As ePeptideHitResultType, ByVal strParamFilePath As String) As Boolean

        Dim blnSuccess As Boolean

        m_ETDMode = False
        blnSuccess = False

        If String.IsNullOrEmpty(strParamFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PeptideHit param file path is empty; unable to continue")
            Return False
        End If

        m_StatusTools.CurrentOperation = "Checking whether ETD mode is enabled"

        Select Case eResultType
            Case ePeptideHitResultType.Sequest
                blnSuccess = CheckETDModeEnabledSequest(strParamFilePath)

            Case ePeptideHitResultType.XTandem
                blnSuccess = CheckETDModeEnabledXTandem(strParamFilePath)

            Case ePeptideHitResultType.Inspect
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inspect does not support ETD data processing; will set m_ETDMode to False")
                blnSuccess = True

            Case Else
                ' Unknown result type
        End Select

        If m_ETDMode Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ETD search mode has been enabled since c and z ions were used for the peptide search")
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Examines the Sequest param file to determine if ETD mode is enabled
    ''' If it is, then sets m_ETDMode to True
    ''' </summary>
    ''' <param name="strParamFilePath">Sequest parameter file to read</param>
    ''' <returns>True if success; false if an error</returns>
    Protected Function CheckETDModeEnabledSequest(ByVal strParamFilePath As String) As Boolean

        Const SEQUEST_ION_SERIES_TAG As String = "ion_series"

        Dim srParamFile As System.IO.StreamReader
        Dim strLineIn As String

        Dim strIonWeightText As String
        Dim strIonWeights() As String

        Dim dblCWeight As Double
        Dim dblZWeight As Double

        Dim strTag As String = String.Empty
        Dim strSetting As String = String.Empty

        Dim intLinesRead As Integer
        Dim intCharIndex As Integer

        Try
            m_ETDMode = False

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Reading the Sequest parameter file: " & strParamFilePath)
            End If

            ' Read the data from the Sequest Param file
            srParamFile = New System.IO.StreamReader(New System.IO.FileStream(strParamFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

            intLinesRead = 0

            Do While srParamFile.Peek >= 0
                strLineIn = srParamFile.ReadLine
                intLinesRead += 1

                If Not String.IsNullOrEmpty(strLineIn) AndAlso _
                   strLineIn.StartsWith(SEQUEST_ION_SERIES_TAG) Then

                    ' This is the ion_series line
                    ' If ETD mode is enabled, then c and z ions will have a 1 in this series of numbers:
                    ' ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0 
                    '
                    ' The key to parsing this data is:
                    ' ion_series = - - -  a   b   c  --- --- ---  x   y   z
                    ' ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0 

                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest " & SEQUEST_ION_SERIES_TAG & " line found: " & strLineIn)
                    End If

                    ' Look for the equals sign
                    intCharIndex = strLineIn.IndexOf("=")
                    If intCharIndex > 0 Then
                        strIonWeightText = strLineIn.Substring(intCharIndex + 1).Trim

                        ' Split strIonWeightText on spaces
                        strIonWeights = strIonWeightText.Split(" "c)

                        If strIonWeights.Length >= 12 Then
                            dblCWeight = 0
                            dblZWeight = 0

                            Double.TryParse(strIonWeights(5), dblCWeight)
                            Double.TryParse(strIonWeights(11), dblZWeight)

                            If m_DebugLevel >= 3 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest " & SEQUEST_ION_SERIES_TAG & " line has c-ion weighting = " & dblCWeight & " and z-ion weighting = " & dblZWeight)
                            End If

                            If dblCWeight > 0 OrElse dblZWeight > 0 Then
                                m_ETDMode = True
                            End If
                        Else
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Sequest " & SEQUEST_ION_SERIES_TAG & " line does not have 11 numbers; will assume not using ETD ions: " & strLineIn)
                        End If
                    Else
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Sequest " & SEQUEST_ION_SERIES_TAG & " line does not have an equals sign; will assume not using ETD ions: " & strLineIn)
                    End If

                    ' No point in checking any further since we've parsed the ion_series line
                    Exit Do

                End If

            Loop

            srParamFile.Close()

        Catch ex As Exception
            Dim Msg As String
            Msg = "Error reading the Sequest param file: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception reading Sequest parameter file")

            If Not srParamFile Is Nothing Then srParamFile.Close()

            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Examines the X!Tndem param file to determine if ETD mode is enabled
    ''' If it is, then sets m_ETDMode to True
    ''' </summary>
    ''' <param name="strParamFilePath">X!Tandem XML parameter file to read</param>
    ''' <returns>True if success; false if an error</returns>
    Protected Function CheckETDModeEnabledXTandem(ByVal strParamFilePath As String) As Boolean

        Dim objParamFile As System.Xml.XmlDocument

        Dim objSelectedNodes As System.Xml.XmlNodeList
        Dim objAttributeNode As System.Xml.XmlNode

        Dim intSettingIndex As Integer
        Dim intMatchIndex As Integer

        Try
            m_ETDMode = False

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Reading the X!Tandem parameter file: " & strParamFilePath)
            End If

            ' Open the parameter file
            ' Look for either of these lines:
            '   <note type="input" label="scoring, c ions">yes</note>
            '   <note type="input" label="scoring, z ions">yes</note>

            objParamFile = New System.Xml.XmlDocument
            objParamFile.PreserveWhitespace = True
            objParamFile.Load(strParamFilePath)

            For intSettingIndex = 0 To 1
                Select Case intSettingIndex
                    Case 0
                        objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, c ions']")
                    Case 1
                        objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, z ions']")
                End Select

                If Not objSelectedNodes Is Nothing Then

                    For intMatchIndex = 0 To objSelectedNodes.Count - 1
                        ' Make sure this node has an attribute named type with value "input"
                        objAttributeNode = objSelectedNodes.Item(intMatchIndex).Attributes.GetNamedItem("type")

                        If objAttributeNode Is Nothing Then
                            ' Node does not have an attribute named "type"
                        Else
                            If objAttributeNode.Value.ToLower = "input" Then
                                ' Valid node; examine its InnerText value
                                If objSelectedNodes.Item(intMatchIndex).InnerText.ToLower() = "yes" Then
                                    m_ETDMode = True
                                End If
                            End If
                        End If
                    Next intMatchIndex

                End If

                If m_ETDMode Then Exit For
            Next intSettingIndex

        Catch ex As Exception

            Dim Msg As String
            Msg = "Error reading the X!Tandem param file: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception reading X!Tandem parameter file")

            Return False
        End Try

        Return True
    End Function

    ''' <summary>
    ''' Creates the MSGF Input file by reading Sequest, X!Tandem, or Inspect PHRP result file and extracting the relevant information
    ''' Uses the ModSummary.txt file to determine the dynamic and static mods used
    ''' </summary>
    ''' <param name="eResultType"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function CreateMSGFInputFile(ByVal eResultType As ePeptideHitResultType) As Boolean

        Dim strModSummaryFilePath As String
        Dim Msg As String

        Dim blnSuccess As Boolean

        ' This list contains mod symbols as the key and the corresponding mod mass (stored as a string 
        '  to retain the same number of Sig Figs as the _ModSummary.txt file)
        ' This dictionary object will use case-sensitive searching
        Dim objDynamicMods As New System.Collections.Generic.SortedDictionary(Of String, String)

        ' This list contains amino acid names as the key and the corresponding mod mass (stored as a string 
        '  to retain the same number of Sig Figs as the _ModSummary.txt file)
        ' This dictionary object will use case-sensitive searching
        Dim objStaticMods As New System.Collections.Generic.SortedDictionary(Of String, String)

        ' Read the PHRP Mod Summary File
        strModSummaryFilePath = System.IO.Path.Combine(m_WorkDir, GetModSummaryFileName(eResultType, m_DatasetName))
        blnSuccess = ReadModSummaryFile(strModSummaryFilePath, objDynamicMods, objStaticMods)

        If blnSuccess Then

            ' Convert the peptide-hit result file (from PHRP) to a tab-delimited input file to be read by MSGF
            Select Case eResultType
                Case ePeptideHitResultType.Sequest

                    ' Convert Sequest results to input format required for MSGF
                    mMSGFInputCreator = New clsMSGFInputCreatorSequest(m_DatasetName, m_WorkDir, objDynamicMods, objStaticMods)

                Case ePeptideHitResultType.XTandem

                    ' Convert X!Tandem results to input format required for MSGF
                    mMSGFInputCreator = New clsMSGFInputCreatorXTandem(m_DatasetName, m_WorkDir, objDynamicMods, objStaticMods)


                Case ePeptideHitResultType.Inspect

                    ' Convert Inspect results to input format required for MSGF
                    mMSGFInputCreator = New clsMSGFInputCreatorInspect(m_DatasetName, m_WorkDir, objDynamicMods, objStaticMods)

                Case Else
                    'Should never get here; invalid result type specified
                    Msg = "Invalid PeptideHit ResultType specified: " & eResultType
                    m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMSGFToolRunner.CreateMSGFInputFile(); " & Msg)

                    blnSuccess = False
            End Select

            If blnSuccess Then

                mMSGFInputFilePath = mMSGFInputCreator.MSGFInputFilePath()
                mMSGFResultsFilePath = mMSGFInputCreator.MSGFResultsFilePath()

                m_StatusTools.CurrentOperation = "Creating the MSGF Input file"

                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating the MSGF Input file")
                End If

                blnSuccess = mMSGFInputCreator.CreateMSGFInputFileUsingPHRPResultFiles()

                mMSGFInputFileLineCount = mMSGFInputCreator.MSGFInputFileLineCount

                If Not blnSuccess Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mMSGFInputCreator.MSGFDataFileLineCount returned False")
                Else
                    If m_DebugLevel >= 2 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CreateMSGFInputFileUsingPHRPResultFile complete; " & mMSGFInputFileLineCount & " lines of data")
                    End If
                End If

            End If

        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Generate the mzXML
    ''' </summary>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Private Function CreateMZXMLFile() As Boolean

        Dim InspectDir As String = m_mgrParams.GetParam("InspectDir")                   ' ReadW.exe is stored in the Inspect folder
        Dim msXmlGenerator As String = "ReadW.exe"
        Dim dtStartTime As System.DateTime

        Dim CentroidMSXML As Boolean = False

        Dim ReadWProgramPath As String
        Dim eOutputType As clsMSXMLGenReadW.MSXMLOutputTypeConstants

        Dim blnSuccess As Boolean

        m_StatusTools.CurrentOperation = "Creating the .mzXML file"

        ' mzXML filename is dataset plus .mzXML
        If System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, m_DatasetName & AnalysisManagerBase.clsAnalysisResources.DOT_MZXML_EXTENSION)) Then
            ' File already exists; nothing to do
            Return True
        End If

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating the .mzXML file for " & m_DatasetName)
        End If

        ReadWProgramPath = System.IO.Path.Combine(InspectDir, msXmlGenerator)
        eOutputType = clsMSXMLGenReadW.MSXMLOutputTypeConstants.mzXML

        ' Instantiate the processing class
        mMSXmlGenReadW = New clsMSXMLGenReadW(m_WorkDir, ReadWProgramPath, m_DatasetName, eOutputType, CentroidMSXML)

        dtStartTime = System.DateTime.Now

        ' Create the file
        blnSuccess = mMSXmlGenReadW.CreateMSXMLFile

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mMSXmlGenReadW.ErrorMessage)
            Return False

        ElseIf mMSXmlGenReadW.ErrorMessage.Length > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mMSXmlGenReadW.ErrorMessage)
        End If

        If m_DebugLevel >= 1 Then
            Try
                ' Save some stats to the log

                Dim strMessage As String
                Dim ioFileInfo As System.IO.FileInfo
                Dim dblFileSizeMB As Double, dblXMLSizeMB As Double
                Dim dblTotalMinutes As Double

                dblTotalMinutes = System.DateTime.Now.Subtract(dtStartTime).TotalMinutes

                ioFileInfo = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, m_DatasetName & AnalysisManagerBase.clsAnalysisResources.DOT_RAW_EXTENSION))
                If ioFileInfo.Exists Then
                    dblFileSizeMB = ioFileInfo.Length / 1024.0 / 1024
                End If

                ioFileInfo = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, m_DatasetName & AnalysisManagerBase.clsAnalysisResources.DOT_MZXML_EXTENSION))
                If ioFileInfo.Exists Then
                    dblXMLSizeMB = ioFileInfo.Length / 1024.0 / 1024
                End If

                strMessage = "mzXML creation time = " & dblTotalMinutes.ToString("0.00") & " minutes"

                If dblTotalMinutes > 0 Then
                    strMessage &= "; Processing rate = " & (dblFileSizeMB / dblTotalMinutes / 60).ToString("0.0") & " MB/second"
                End If

                strMessage &= "; .Raw file size = " & dblFileSizeMB.ToString("0.0") & " MB"
                strMessage &= "; .mzXML file size = " & dblXMLSizeMB.ToString("0.0") & " MB"

                If dblFileSizeMB > 0 Then
                    strMessage &= "; Filesize Ratio = " & (dblXMLSizeMB / dblFileSizeMB).ToString("0.00")
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception saving mzXML stats: " & ex.Message)
            End Try
        End If

        Return True

    End Function

    Public Shared Function GetPeptideHitResultType(ByVal strPeptideHitResultType As String) As ePeptideHitResultType
        Select Case strPeptideHitResultType.ToLower
            Case "Peptide_Hit".ToLower
                Return ePeptideHitResultType.Sequest

            Case "XT_Peptide_Hit".ToLower
                Return ePeptideHitResultType.XTandem

            Case "IN_Peptide_Hit".ToLower
                Return ePeptideHitResultType.Inspect

            Case Else
                Return ePeptideHitResultType.Unknown
        End Select
    End Function

    Public Shared Function GetModSummaryFileName(ByVal eResultType As ePeptideHitResultType, ByVal strDatasetName As String) As String

        Dim strModSummaryName As String = String.Empty

        Select Case eResultType
            Case ePeptideHitResultType.Sequest
                ' Sequest: _syn_ModSummary.txt
                strModSummaryName = strDatasetName & "_syn_ModSummary.txt"

            Case ePeptideHitResultType.XTandem
                ' X!Tandem: _xt_ModSummary.txt
                strModSummaryName = strDatasetName & "_xt_ModSummary.txt"

            Case ePeptideHitResultType.Inspect
                ' Inspect: _inspect_syn_ModSummary.txt
                strModSummaryName = strDatasetName & "_inspect_syn_ModSummary.txt"

        End Select

        Return strModSummaryName

    End Function

    Public Shared Function GetPHRPFirstHitsFileName(ByVal eResultType As ePeptideHitResultType, ByVal strDatasetName As String) As String

        Dim strPHRPResultsFileName As String = String.Empty

        Select Case eResultType
            Case ePeptideHitResultType.Sequest
                ' Sequest: _fht.txt
                strPHRPResultsFileName = clsMSGFInputCreatorSequest.GetPHRPFirstHitsFileName(strDatasetName)

            Case ePeptideHitResultType.XTandem
                ' X!Tandem does not have a first-hits file; strPHRPResultsFileName will be an empty string
                strPHRPResultsFileName = clsMSGFInputCreatorXTandem.GetPHRPFirstHitsFileName(strDatasetName)

            Case ePeptideHitResultType.Inspect
                ' Inspect: _inspect_fht.txt
                strPHRPResultsFileName = clsMSGFInputCreatorInspect.GetPHRPFirstHitsFileName(strDatasetName)

        End Select

        Return strPHRPResultsFileName

    End Function

    Public Shared Function GetPHRPSynopsisFileName(ByVal eResultType As ePeptideHitResultType, ByVal strDatasetName As String) As String

        Dim strPHRPResultsFileName As String = String.Empty

        Select Case eResultType
            Case ePeptideHitResultType.Sequest
                ' Sequest: _syn.txt
                strPHRPResultsFileName = clsMSGFInputCreatorSequest.GetPHRPSynopsisFileName(strDatasetName)

            Case ePeptideHitResultType.XTandem
                ' X!Tandem: _xt.txt
                strPHRPResultsFileName = clsMSGFInputCreatorXTandem.GetPHRPSynopsisFileName(strDatasetName)

            Case ePeptideHitResultType.Inspect
                ' Inspect: _inspect_syn.txt
                strPHRPResultsFileName = clsMSGFInputCreatorInspect.GetPHRPSynopsisFileName(strDatasetName)

        End Select

        Return strPHRPResultsFileName

    End Function


    Protected Function LoadXTandemResultProteins(ByRef objProteinByResultID As System.Collections.Generic.SortedList(Of Integer, String)) As Boolean

        ' Tracks the ResultIDs that map to each SeqID
        Dim objSeqToResultMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of Integer))

        Dim blnSuccess As Boolean

        Try
            ' Initialize the tracking lists
            objSeqToResultMap = New System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of Integer))

            If objProteinByResultID Is Nothing Then
                objProteinByResultID = New System.Collections.Generic.SortedList(Of Integer, String)
            End If

            blnSuccess = LoadXTandemResultToSeqMapping(objSeqToResultMap)

            If blnSuccess Then
                blnSuccess = LoadXTandemSeqToProteinMapping(objSeqToResultMap, objProteinByResultID)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error loading X!Tandem protein results: " & ex.Message)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception loading X!Tandem protein results")
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function


    Protected Function LoadXTandemResultToSeqMapping(ByRef objSeqToResultMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of Integer))) As Boolean

        Dim srInFile As System.IO.StreamReader

        Dim objResultIDList As System.Collections.Generic.List(Of Integer)

        Dim strFilePath As String
        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim intLinesRead As Integer

        Dim intResultID As Integer
        Dim intSeqID As Integer

        Try

            ' Read the data from the result to sequence map file
            strFilePath = System.IO.Path.Combine(m_WorkDir, m_DatasetName & XT_RESULT_TO_SEQ_MAP_SUFFIX)
            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

            intLinesRead = 0

            Do While srInFile.Peek >= 0
                strLineIn = srInFile.ReadLine
                intLinesRead += 1

                If Not String.IsNullOrEmpty(strLineIn) Then
                    strSplitLine = strLineIn.Split(ControlChars.Tab)

                    If strSplitLine.Length >= 2 Then

                        ' Parse out the numbers from the first two columns 
                        ' (the first line of the file is the header line, and it will get skipped)
                        If Integer.TryParse(strSplitLine(0), intResultID) Then
                            If Integer.TryParse(strSplitLine(1), intSeqID) Then

                                If objSeqToResultMap.TryGetValue(intSeqID, objResultIDList) Then
                                    objResultIDList.Add(intResultID)
                                Else
                                    objResultIDList = New System.Collections.Generic.List(Of Integer)
                                    objResultIDList.Add(intResultID)
                                    objSeqToResultMap.Add(intSeqID, objResultIDList)
                                End If
                            End If
                        End If

                    End If
                End If
            Loop

            srInFile.Close()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading X!Tandem result to seq map file: " & ex.Message)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception reading X!Tandem " & XT_RESULT_TO_SEQ_MAP_SUFFIX & " file")
            Return False
        End Try

        Return True

    End Function


    Protected Function LoadXTandemSeqToProteinMapping(ByRef objSeqToResultMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of Integer)), _
                                                      ByRef objProteinByResultID As System.Collections.Generic.SortedList(Of Integer, String)) As Boolean

        Dim srInFile As System.IO.StreamReader

        Dim objResultIDList As System.Collections.Generic.List(Of Integer)

        Dim objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

        Dim strFilePath As String
        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim strProtein As String
        Dim intLinesRead As Integer

        Dim intResultID As Integer
        Dim intSeqID As Integer
        Dim intSeqIDPrevious As Integer

        Dim blnHeaderLineParsed As Boolean
        Dim blnSkipLine As Boolean

        Try

            ' Initialize the column mapping
            ' Using a case-insensitive comparer
            objColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

            ' Define the default column mapping
            objColumnHeaders.Add(XT_SEQ_PROT_MAP_COLUMN_Unique_Seq_ID, 0)
            objColumnHeaders.Add(XT_SEQ_PROT_MAP_COLUMN_Cleavage_State, 1)
            objColumnHeaders.Add(XT_SEQ_PROT_MAP_COLUMN_Terminus_State, 2)
            objColumnHeaders.Add(XT_SEQ_PROT_MAP_COLUMN_Protein_Name, 3)
            objColumnHeaders.Add(XT_SEQ_PROT_MAP_COLUMN_Protein_EValue, 4)
            objColumnHeaders.Add(XT_SEQ_PROT_MAP_COLUMN_Protein_Intensity, 5)

            ' Read the data from the sequence to protein map file
            strFilePath = System.IO.Path.Combine(m_WorkDir, m_DatasetName & XT_SEQ_TO_PROTEIN_MAP_SUFFIX)
            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

            intLinesRead = 0
            intSeqIDPrevious = 0

            Do While srInFile.Peek >= 0
                strLineIn = srInFile.ReadLine
                intLinesRead += 1
                blnSkipLine = False

                If Not String.IsNullOrEmpty(strLineIn) Then
                    strSplitLine = strLineIn.Split(ControlChars.Tab)

                    If Not blnHeaderLineParsed Then
                        If strSplitLine(0).ToLower() = XT_SEQ_PROT_MAP_COLUMN_Unique_Seq_ID.ToLower Then
                            ' Parse the header line to confirm the column ordering
                            clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, objColumnHeaders)
                            blnSkipLine = True
                        End If

                        blnHeaderLineParsed = True
                    End If

                    If Not blnSkipLine AndAlso strSplitLine.Length >= 3 Then

                        If Integer.TryParse(strSplitLine(0), intSeqID) Then
                            If intSeqID <> intSeqIDPrevious Then
                                strProtein = clsMSGFInputCreator.LookupColumnValue(strSplitLine, XT_SEQ_PROT_MAP_COLUMN_Protein_Name, objColumnHeaders, String.Empty)

                                If Not String.IsNullOrEmpty(strProtein) Then
                                    ' Find the ResultIDs in objResultToSeqMap() that have sequence ID intSeqID
                                    If objSeqToResultMap.TryGetValue(intSeqID, objResultIDList) Then

                                        For Each intResultID In objResultIDList
                                            If Not objProteinByResultID.ContainsKey(intResultID) Then
                                                objProteinByResultID.Add(intResultID, strProtein)
                                            End If
                                        Next

                                    End If

                                End If

                            End If
                        End If

                    End If

                End If
            Loop

            srInFile.Close()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading X!Tandem seq to protein map file: " & ex.Message)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception reading X!Tandem " & XT_SEQ_TO_PROTEIN_MAP_SUFFIX & " file")
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Post-process the MSGF output file to create two new MSGF result files, one for the synopsis file and one for the first-hits file
    ''' Will also look for non-numeric values in the SpecProb column
    ''' Examples:
    '''   N/A: unrecognizable annotation
    '''   N/A: precursor mass != peptide mass (4089.068 vs 4078.069)
    ''' the new MSGF result files will guarantee that the SpecProb column has a number, 
    '''   but will have an additional column called SpecProbNotes with any notes or warnings
    ''' The synopsis-based MSGF results will be extended to include any entries skipped when
    '''  creating the MSGF input file (to aid in linking up files later)
    ''' </summary>
    ''' <param name="strMSGFResultsFilePath">MSGF results file to examine</param>
    ''' <returns>True if success; false if one or more errors</returns>
    ''' <remarks></remarks>
    Protected Function PostProcessMSGFResults(ByVal eResultType As ePeptideHitResultType, _
                                              ByVal strMSGFResultsFilePath As String) As Boolean

        Const MAX_ERRORS_TO_LOG As Integer = 5

        ' If 10% or more of the data has a message like "N/A: precursor mass != peptide mass (3571.8857 vs 3581.9849)"
        '  then return false
        Const MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT As Integer = 10

        Dim chSepChars() As Char = New Char() {ControlChars.Tab}

        Dim srMSGFResults As System.IO.StreamReader
        Dim swMSGFSynFile As System.IO.StreamWriter

        Dim blnFirstHitsDataPresent As Boolean

        Dim fiInputFile As System.IO.FileInfo
        Dim fiMSGFSynFile As System.IO.FileInfo

        Dim strMSGFSynopsisResults As String = String.Empty

        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

        Dim intLinesRead As Integer
        Dim intErrorCount As Integer
        Dim intPrecursorMassErrorCount As Integer

        Dim strOriginalPeptide As String
        Dim strScan As String
        Dim strCharge As String
        Dim strProtein As String
        Dim strPeptide As String
        Dim strResultID As String
        Dim strSpecProb As String
        Dim strDataSource As String
        Dim strNotes As String

        Dim strMSGFResultData As String
        Dim strOriginalPeptideInfo As String
        Dim strProteinNew As String

        Dim intResultID As Integer
        Dim intIndex As Integer
        Dim objSkipList As System.Collections.Generic.List(Of String)
        Dim strSkipInfo() As String

        Dim objProteinByResultID As System.Collections.Generic.SortedList(Of Integer, String)

        Dim Msg As String
        Dim sngPercentDataPrecursorMassError As Single
        Dim blnSuccess As Boolean

        Dim blnSkipLine As Boolean
        Dim blnHeaderLineParsed As Boolean

        Try
            If String.IsNullOrEmpty(strMSGFResultsFilePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSGF Results File path is empty; unable to continue")
                Return False
            End If

            m_StatusTools.CurrentOperation = "MSGF complete; post-processing the results"

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGF complete; post-processing the results")
            End If

            objProteinByResultID = New System.Collections.Generic.SortedList(Of Integer, String)

            If eResultType = ePeptideHitResultType.XTandem Then
                ' Need to read the ResultToSeqMap and SeqToProteinMap files so that we can determine the first protein name for each result
                blnSuccess = LoadXTandemResultProteins(objProteinByresultID)
                If Not blnSuccess Then
                    Return False
                End If

            End If

            fiInputFile = New System.IO.FileInfo(strMSGFResultsFilePath)

            ' Define the path to write the synopsis MSGF results to
            strMSGFSynopsisResults = System.IO.Path.Combine(fiInputFile.DirectoryName, _
                                                             System.IO.Path.GetFileNameWithoutExtension(fiInputFile.Name) & "_PostProcess.txt")


            m_progress = PROGRESS_PCT_MSGF_POST_PROCESSING
            m_StatusTools.UpdateAndWrite(m_progress)

            ' Initialize the column mapping
            ' Using a case-insensitive comparer
            objColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

            ' Define the default column mapping
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_SpectrumFile, 0)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_Title, 1)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_ScanNumber, 2)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_Annotation, 3)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_Charge, 4)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_Protein_First, 5)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_Result_ID, 6)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_Data_Source, 7)
            objColumnHeaders.Add(MSGF_RESULT_COLUMN_SpecProb, 8)

            ' Read the data from the MSGF Result file
            srMSGFResults = New System.IO.StreamReader(New System.IO.FileStream(strMSGFResultsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

            ' Create a new file for writing the Synopsis MSGF Results
            swMSGFSynFile = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFSynopsisResults, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

            ' Write out the headers to swMSGFSynFile
            mMSGFInputCreator.WriteMSGFResultsHeaders(swMSGFSynFile)

            blnHeaderLineParsed = False
            blnFirstHitsDataPresent = False

            intLinesRead = 0
            intErrorCount = 0
            intPrecursorMassErrorCount = 0

            Do While srMSGFResults.Peek >= 0
                strLineIn = srMSGFResults.ReadLine
                intLinesRead += 1
                blnSkipLine = False

                If Not String.IsNullOrEmpty(strLineIn) Then
                    strSplitLine = strLineIn.Split(ControlChars.Tab)

                    If Not blnHeaderLineParsed Then
                        If strSplitLine(0).ToLower() = MSGF_RESULT_COLUMN_SpectrumFile.ToLower Then
                            ' Parse the header line to confirm the column ordering
                            clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, objColumnHeaders)
                            blnSkipLine = True
                        End If

                        blnHeaderLineParsed = True
                    End If

                    If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then

                        strOriginalPeptide = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Title, objColumnHeaders)
                        strScan = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_ScanNumber, objColumnHeaders)
                        strCharge = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Charge, objColumnHeaders)
                        strProtein = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Protein_First, objColumnHeaders)
                        strPeptide = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Annotation, objColumnHeaders)
                        strResultID = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Result_ID, objColumnHeaders)
                        strSpecProb = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_SpecProb, objColumnHeaders)
                        strDataSource = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Data_Source, objColumnHeaders)
                        strNotes = String.Empty

                        If eResultType = ePeptideHitResultType.XTandem Then
                            ' Update the protein name
                            If Integer.TryParse(strResultID, intResultID) Then
                                If objProteinByResultID.TryGetValue(intResultID, strProteinNew) Then
                                    strProtein = strProteinNew
                                End If
                            End If
                        End If

                        If Not Double.TryParse(strSpecProb, 0.0) Then
                            ' The specProb column does not contain a number
                            intErrorCount += 1

                            If intErrorCount <= MAX_ERRORS_TO_LOG Then
                                ' Log the first 5 instances to the log file as warnings

                                If strOriginalPeptide <> strPeptide Then
                                    strOriginalPeptideInfo = ", original peptide sequence " & strOriginalPeptide
                                Else
                                    strOriginalPeptideInfo = String.Empty
                                End If

                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF SpecProb is not numeric on line " & intLinesRead & " in the result file: " & strSpecProb & " (parent peptide " & strPeptide & ", Scan " & strScan & ", Result_ID " & strResultID & strOriginalPeptideInfo & ")")
                            End If

                            If strSpecProb.Contains("precursor mass") Then
                                intPrecursorMassErrorCount += 1
                            End If

                            If strOriginalPeptide <> strPeptide Then
                                strNotes = strPeptide & "; " & strSpecProb
                            Else
                                strNotes = String.Copy(strSpecProb)
                            End If

                            ' Change the spectrum probability to 1
                            strSpecProb = "1"
                        Else
                            If strOriginalPeptide <> strPeptide Then
                                strNotes = String.Copy(strPeptide)
                            End If
                        End If

                        strMSGFResultData = strScan & ControlChars.Tab & _
                                            strCharge & ControlChars.Tab & _
                                            strProtein & ControlChars.Tab & _
                                            strOriginalPeptide & ControlChars.Tab & _
                                            strSpecProb & ControlChars.Tab & _
                                            strNotes

                        ' Add this result to the cached string dictionary
                        mMSGFInputCreator.AddUpdateMSGFResult(strScan, strCharge, strOriginalPeptide, strMSGFResultData)


                        If strDataSource = MSGF_PHRP_DATA_SOURCE_FHT Then
                            ' First-hits file
                            blnFirstHitsDataPresent = True

                        Else
                            ' Synopsis file

                            ' Add this entry to the MSGF synopsis results
                            ' Note that strOriginalPeptide has the original peptide sequence
                            swMSGFSynFile.WriteLine(strResultID & ControlChars.Tab & strMSGFResultData)

                            ' See if any entries were skipped when reading the synopsis file used to create the MSGF input file
                            ' If they were, add them to the validated MSGF file (to aid in linking up files later)

                            If Integer.TryParse(strResultID, intResultID) Then
                                objSkipList = mMSGFInputCreator.GetSkippedInfoByResultId(intResultID)

                                For intIndex = 0 To objSkipList.Count - 1

                                    ' Split the entry on the tab character
                                    ' The item left of the tab is the skipped result id
                                    ' the item right of the tab is the protein corresponding to the skipped result id

                                    strSkipInfo = objSkipList(intIndex).Split(chSepChars, 2)

                                    swMSGFSynFile.WriteLine(strSkipInfo(0) & ControlChars.Tab & _
                                                              strScan & ControlChars.Tab & _
                                                              strCharge & ControlChars.Tab & _
                                                              strSkipInfo(1) & ControlChars.Tab & _
                                                              strOriginalPeptide & ControlChars.Tab & _
                                                              strSpecProb & ControlChars.Tab & _
                                                              strNotes)

                                Next
                            End If
                        End If

                    End If
                End If

            Loop

            srMSGFResults.Close()
            swMSGFSynFile.Close()

            If intErrorCount > 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF SpecProb was not numeric for " & intErrorCount & " entries in the MSGF result file")
            End If

        Catch ex As Exception
            Msg = "Error post-processing the MSGF Results file: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception post-processing the MSGF Results file")

            If Not srMSGFResults Is Nothing Then srMSGFResults.Close()
            If Not swMSGFSynFile Is Nothing Then swMSGFSynFile.Close()

            Return False
        End Try

        Try
            ' Now replace the _MSGF.txt file with the _MSGF_validated.txt file
            ' For example, replace:
            '   QC_Shew_Dataset_syn_MSGF.txt
            ' With:
            '   QC_Shew_Dataset_syn_MSGF_validated.txt

            System.Threading.Thread.Sleep(500)

            fiInputFile.Delete()
            System.Threading.Thread.Sleep(500)

            fiMSGFSynFile = New System.IO.FileInfo(strMSGFSynopsisResults)

            fiMSGFSynFile.MoveTo(strMSGFResultsFilePath)

        Catch ex As Exception
            Msg = "Error replacing the original MSGF Results file with the post-processed one: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception post-processing the MSGF Results file")

            Return False
        End Try

        Try
            ' Compare intPrecursorMassErrorCount to intLinesRead
            ' If more than 10% of the results have a precursor mass error, then return false
            blnSuccess = True

            If intLinesRead >= 2 AndAlso intPrecursorMassErrorCount > 0 Then
                sngPercentDataPrecursorMassError = CSng(intPrecursorMassErrorCount / intLinesRead * 100)

                Msg = sngPercentDataPrecursorMassError.ToString("0.0") & "% of the data processed by MSGF has a precursor mass 10 or more Da away from the computed peptide mass"

                If sngPercentDataPrecursorMassError >= MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT Then
                    Msg &= "; this likely indicates a static or dynamic mod definition is missing from the PHRP _ModSummary.txt file"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    blnSuccess = False
                Else
                    Msg &= "; this is below the error threshold of " & MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT & "% and thus is only a warning (note that static and dynamic mod info is loaded from the PHRP _ModSummary.txt file)"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
                    blnSuccess = True
                End If
            End If

        Catch ex As Exception
            ' Ignore errors here
        End Try

        If blnSuccess AndAlso blnFirstHitsDataPresent Then
            ' Write out the First-Hits file results
            blnSuccess = mMSGFInputCreator.CreateMSGFFirstHitsFile()
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Reads the data in strModSummaryFilePath.  Populates objDynamicMods and objStaticMods with the modification definitions
    ''' </summary>
    ''' <param name="strModSummaryFilePath">Path to the PHRP Mod Summary file to read</param>
    ''' <param name="objDynamicMods">List with mod symbols as the key and the corresponding mod mass</param>
    ''' <param name="objStaticMods">List with amino acid names as the key and the corresponding mod mass</param>
    ''' <returns>True if success; false if an error</returns>
    Protected Function ReadModSummaryFile(ByVal strModSummaryFilePath As String, _
                                          ByRef objDynamicMods As System.Collections.Generic.SortedDictionary(Of String, String), _
                                          ByRef objStaticMods As System.Collections.Generic.SortedDictionary(Of String, String)) As Boolean

        Dim srModSummaryFile As System.IO.StreamReader
        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

        Dim intLinesRead As Integer
        Dim intIndex As Integer

        Dim strModSymbol As String
        Dim strModMass As String
        Dim strTargetResidues As String
        Dim strModType As String

        Dim blnSkipLine As Boolean
        Dim blnHeaderLineParsed As Boolean

        Try
            If String.IsNullOrEmpty(strModSummaryFilePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ModSummaryFile path is empty; unable to continue")
                Return False
            End If

            m_StatusTools.CurrentOperation = "Reading the PHRP ModSummary file"

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Reading the PHRP ModSummary file: " & strModSummaryFilePath)
            End If

            ' Initialize the column mapping
            ' Using a case-insensitive comparer
            objColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

            ' Define the default column mapping
            objColumnHeaders.Add(MOD_SUMMARY_COLUMN_Modification_Symbol, 0)
            objColumnHeaders.Add(MOD_SUMMARY_COLUMN_Modification_Mass, 1)
            objColumnHeaders.Add(MOD_SUMMARY_COLUMN_Target_Residues, 2)
            objColumnHeaders.Add(MOD_SUMMARY_COLUMN_Modification_Type, 3)
            objColumnHeaders.Add(MOD_SUMMARY_COLUMN_Mass_Correction_Tag, 4)
            objColumnHeaders.Add(MOD_SUMMARY_COLUMN_Occurence_Count, 5)

            ' Clear objDynamicMods and objStaticMods (should have been instantiated by the calling function)
            objDynamicMods.Clear()
            objStaticMods.Clear()


            ' Read the data from the ModSummary.txt file
            ' The first line is typically a header line:
            ' Modification_Symbol	Modification_Mass	Target_Residues	Modification_Type	Mass_Correction_Tag	Occurence_Count

            srModSummaryFile = New System.IO.StreamReader(New System.IO.FileStream(strModSummaryFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

            blnHeaderLineParsed = False
            intLinesRead = 0

            Do While srModSummaryFile.Peek >= 0
                strLineIn = srModSummaryFile.ReadLine
                intLinesRead += 1
                blnSkipLine = False

                If Not String.IsNullOrEmpty(strLineIn) Then
                    strSplitLine = strLineIn.Split(ControlChars.Tab)

                    If Not blnHeaderLineParsed Then
                        If strSplitLine(0).ToLower() = MOD_SUMMARY_COLUMN_Modification_Symbol.ToLower Then
                            ' Parse the header line to confirm the column ordering
                            clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, objColumnHeaders)
                            blnSkipLine = True
                        End If

                        blnHeaderLineParsed = True
                    End If

                    If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then
                        strModSymbol = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MOD_SUMMARY_COLUMN_Modification_Symbol, objColumnHeaders)
                        strModMass = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MOD_SUMMARY_COLUMN_Modification_Mass, objColumnHeaders)
                        strTargetResidues = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MOD_SUMMARY_COLUMN_Target_Residues, objColumnHeaders)
                        strModType = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MOD_SUMMARY_COLUMN_Modification_Type, objColumnHeaders)

                        Select Case strModType.ToUpper()
                            Case "S", "T", "P"
                                ' Static residue mod, peptide terminus static mod, or protein terminus static mod
                                ' Note that < and > mean peptide N and C terminus (N_TERMINAL_PEPTIDE_SYMBOL_DMS and C_TERMINAL_PEPTIDE_SYMBOL_DMS)
                                ' Note that [ and ] mean protein N and C terminus (N_TERMINAL_PROTEIN_SYMBOL_DMS and C_TERMINAL_PROTEIN_SYMBOL_DMS)

                                ' This mod could apply to multiple residues, so need to process each character in strTargetResidues
                                For intIndex = 0 To strTargetResidues.Length - 1
                                    Try
                                        If objStaticMods.ContainsKey(strTargetResidues.Chars(intIndex)) Then
                                            ' Residue is already present in objStaticMods; this is unexpected
                                            ' We'll log a warning, but continue
                                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Residue '" & strTargetResidues.Chars(intIndex) & "' has more than one static mod defined; this is not allowed (duplicate has ModMass=" & strModMass & ")")
                                        Else
                                            objStaticMods.Add(strTargetResidues.Chars(intIndex), strModMass)
                                        End If

                                    Catch ex As Exception
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception adding static mod for " & strTargetResidues.Chars(intIndex) & " with ModMass=" & strModMass & ": " & ex.Message)
                                    End Try
                                Next intIndex

                            Case Else
                                ' Dynamic residue mod (Includes mod type "D")
                                ' Note that < and > mean peptide N and C terminus (N_TERMINAL_PEPTIDE_SYMBOL_DMS and C_TERMINAL_PEPTIDE_SYMBOL_DMS)

                                Try
                                    If objDynamicMods.ContainsKey(strModSymbol) Then
                                        ' Mod symbol already present in objDynamicMods; this is unexpected
                                        ' We'll log a warning, but continue
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Dynamic mod symbol '" & strModSymbol & "' is already defined; it cannot have more than one associated mod mass (duplicate has ModMass=" & strModMass & ")")
                                    Else
                                        objDynamicMods.Add(strModSymbol, strModMass)
                                    End If

                                Catch ex As Exception
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception adding dynamic mod for " & strModSymbol & " with ModMass=" & strModMass & ": " & ex.Message)
                                End Try

                        End Select
                    End If
                End If

            Loop

            srModSummaryFile.Close()

        Catch ex As Exception
            Dim Msg As String
            Msg = "Error reading the PHRP Mod Summary file: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception reading PHRP Mod Summary file")

            If Not srModSummaryFile Is Nothing Then srModSummaryFile.Close()

            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Looks for file strFileNameToFind in the transfer folder for this job
    ''' If found, copies the file to the work directory
    ''' </summary>
    ''' <param name="strFileNameToFind"></param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Protected Function RetrievePreGeneratedDataFile(ByVal strFileNameToFind As String) As Boolean

        Dim strTransferFolderPath As String
        Dim strInputFolderName As String
        Dim strFolderToCheck As String = "??"
        Dim strFilePathSource As String
        Dim strFilePathTarget As String

        Try
            strTransferFolderPath = m_jobParams.GetParam("transferFolderPath")
            strInputFolderName = m_jobParams.GetParam("inputFolderName")

            strFolderToCheck = System.IO.Path.Combine(System.IO.Path.Combine(strTransferFolderPath, m_DatasetName), strInputFolderName)

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for folder " & strFolderToCheck)
            End If

            ' Look for strFileNameToFind in strFolderToCheck
            If System.IO.Directory.Exists(strFolderToCheck) Then
                strFilePathSource = System.IO.Path.Combine(strFolderToCheck, strFileNameToFind)

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for file " & strFilePathSource)
                End If

                If System.IO.File.Exists(strFilePathSource) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file " & strFilePathSource & " to " & strFilePathTarget)

                    strFilePathTarget = System.IO.Path.Combine(m_WorkDir, strFileNameToFind)

                    System.IO.File.Copy(strFilePathSource, strFilePathTarget, True)

                    ' File found and successfully copied; return true
                    Return True
                End If
            End If
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception finding file " & strFileNameToFind & " in folder " & strFolderToCheck & ": " & ex.Message)
            Return False
        End Try

        ' File not found
        Return False

    End Function

    Protected Function RunMSGF() As Boolean

        Dim CmdStr As String
        Dim intJavaMemorySize As Integer

        ' Note that mMSGFInputFilePath and mMSGFResultsFilePath get populated by CreateMSGFInputFile

        If String.IsNullOrEmpty(mMSGFInputFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mMSGFInputFilePath has not been defined; unable to continue")
            Return False
        End If

        If String.IsNullOrEmpty(mMSGFResultsFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mMSGFResultsFilePath has not been defined; unable to continue")
            Return False
        End If

        mMSGFRunner = New clsRunDosProgram(m_WorkDir)

        ' verify that the program file exists
        ' progLoc will typically be "C:\Program Files\Java\jre6\bin\Java.exe"
        ' Note that we need to run MSGF with a 64-bit version of Java since it prefers to use 2 or more GB of ram
        Dim progLoc As String = m_mgrParams.GetParam("JavaLoc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'JavaLoc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Java: " & progLoc)
            Return False
        End If

        Dim MSGFLoc As String = m_mgrParams.GetParam("MSGFLoc")
        If Not System.IO.File.Exists(MSGFLoc) Then
            If MSGFLoc.Length = 0 Then MSGFLoc = "Parameter 'MSGFLoc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find MSGF program: " & MSGFLoc)
            Return False
        End If

        ' If an MSGF analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
        ' Customize this on a per-job basis using the MSGFJavaMemorySize setting in the settings file 
        ' (job 611216 succeeded with a value of 5000)
        intJavaMemorySize = clsGlobal.GetJobParameter(m_jobParams, "MSGFJavaMemorySize", 2000)
        If intJavaMemorySize < 512 Then intJavaMemorySize = 512

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSGF")
        End If

        m_progress = PROGRESS_PCT_MSGF_START
        m_StatusTools.CurrentOperation = "Running MSGF"
        m_StatusTools.UpdateAndWrite(m_progress)

        CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & MSGFLoc
        CmdStr &= " -i " & mMSGFInputFilePath       ' Input file
        CmdStr &= " -d " & m_WorkDir                                                                ' Folder containing .mzXML file
        CmdStr &= " -o " & mMSGFResultsFilePath     ' Output file

        If m_ETDMode Then
            CmdStr &= " -m 1"   ' ETD fragmentation
        Else
            CmdStr &= " -m 0"   ' CID fragmentation
        End If

        CmdStr &= " -e 1"       ' Enzyme is Trypsin
        CmdStr &= " -fixMod 0"  ' No fixed mods on cysteine
        CmdStr &= " -x 0"       ' Write out all matches for each spectrum
        CmdStr &= " -p 1"       ' SpecProbThreshold threshold of 1, i.e., do not filter results by the computed SpecProb value

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)

        With mMSGFRunner
            .CreateNoWindow = False
            .CacheStandardOutput = False
            .EchoOutputToConsole = False

            .WriteConsoleOutputToFile = False
            .ConsoleOutputFilePath = ""
        End With

        If Not mMSGFRunner.RunProgram(progLoc, CmdStr, "MSGF", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running MSGF, job " & m_JobNum)
            Return False
        End If

        Return True

    End Function

    Protected Sub UpdateMSGFProgress(ByVal strMSGFResultsFilePath As String)

        Static intErrorCount As Integer = 0

        Dim srMSGFResultsFile As System.IO.StreamReader
        Dim intLineCount As Integer
        Dim dblProgress As Double

        Try

            If mMSGFInputFileLineCount <= 0 Then Exit Sub

            ' Read the data from the Param file
            srMSGFResultsFile = New System.IO.StreamReader(New System.IO.FileStream(strMSGFResultsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

            intLineCount = 0

            Do While srMSGFResultsFile.Peek >= 0
                srMSGFResultsFile.ReadLine()
                intLineCount += 1
            Loop

            srMSGFResultsFile.Close()

            ' Update the overall progress
            dblProgress = intLineCount / mMSGFInputFileLineCount

            m_progress = CSng(PROGRESS_PCT_MSGF_START + (PROGRESS_PCT_MSGF_COMPLETE - PROGRESS_PCT_MSGF_START) * dblProgress)
            m_StatusTools.UpdateAndWrite(m_progress)

        Catch ex As Exception
            ' Log errors the first 3 times they occur
            intErrorCount += 1
            If intErrorCount <= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error counting the number of lines in the MSGF results file, " & strMSGFResultsFilePath & ": " & ex.Message)
            End If
        End Try
    End Sub

#End Region

#Region "Event Handlers"
    ''' <summary>
    ''' Event handler for MSXmlGenReadW.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub MSXmlGenReadW_LoopWaiting() Handles mMSXmlGenReadW.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_progress = PROGRESS_PCT_MSXML_GEN_RUNNING
            m_StatusTools.UpdateAndWrite(m_progress)
        End If
    End Sub

    ''' <summary>
    ''' Event handler for mMSXmlGenReadW.ProgRunnerStarting event
    ''' </summary>
    ''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
    ''' <remarks></remarks>
    Private Sub mMSXmlGenReadW_ProgRunnerStarting(ByVal CommandLine As String) Handles mMSXmlGenReadW.ProgRunnerStarting
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, CommandLine)
    End Sub

    ''' <summary>
    ''' Event handler for Error Events reported by the MSGF Input Creator
    ''' </summary>
    ''' <param name="strErrorMessage"></param>
    ''' <remarks></remarks>
    Private Sub mMSGFInputCreator_ErrorEvent(ByVal strErrorMessage As String) Handles mMSGFInputCreator.ErrorEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reported by MSGFInputCreator; " & strErrorMessage)
    End Sub

    ''' <summary>
    ''' Event handler that fires while MSGF is processing
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub mMSGFRunner_LoopWaiting() Handles mMSGFRunner.LoopWaiting
        Static dtLastUpdateTime As System.DateTime = System.DateTime.Now()

        If System.DateTime.Now.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
            ' Update the MSGF progress by counting the number of lines in the _MSGF.txt file
            UpdateMSGFProgress(mMSGFResultsFilePath)

            dtLastUpdateTime = System.DateTime.Now
        End If
    End Sub
#End Region

End Class
