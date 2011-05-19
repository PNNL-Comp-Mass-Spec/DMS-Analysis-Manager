Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesDtaRefinery
    Inherits clsAnalysisResources

    Friend Const XTANDEM_DEFAULT_INPUT_FILE As String = "xtandem_default_input.xml"
    Friend Const XTANDEM_TAXONOMY_LIST_FILE As String = "xtandem_taxonomy_list.xml"
    Friend Const DTA_REFINERY_INPUT_FILE As String = "DtaRefinery_input.xml"
    Protected WithEvents CmdRunner As clsRunDosProgram

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim result As Boolean
        Dim strErrorMessage As String

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'This will eventually be replaced by Ken Auberry dll call to make param file on the fly

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        Dim strParamFileStoragePathKeyName As String
        Dim strDtaRefineryParmFileStoragePath As String
        strParamFileStoragePathKeyName = AnalysisManagerBase.clsAnalysisMgrSettings.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "DTA_Refinery"

        strDtaRefineryParmFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
        If strDtaRefineryParmFileStoragePath Is Nothing OrElse strDtaRefineryParmFileStoragePath.Length = 0 Then
            strDtaRefineryParmFileStoragePath = "\\gigasax\dms_parameter_Files\DTARefinery"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strDtaRefineryParmFileStoragePath)
        End If

        'Retrieve settings files aka default file that will have values overwritten by parameter file values
        'Stored in same location as parameter file
        If Not RetrieveFile(XTANDEM_DEFAULT_INPUT_FILE, strDtaRefineryParmFileStoragePath, m_mgrParams.GetParam("workdir")) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveFile(XTANDEM_TAXONOMY_LIST_FILE, strDtaRefineryParmFileStoragePath, m_mgrParams.GetParam("workdir")) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveFile(m_jobParams.GetParam("DTARefineryXMLFile"), strDtaRefineryParmFileStoragePath, m_mgrParams.GetParam("workdir")) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Retrieve unzipped dta files (do not unconcatenate since DTA Refinery reads the _DTA.txt file)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve DeconMSn Log file and DeconMSn Profile File
        If Not RetrieveDeconMSnLogFiles() Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        'clsGlobal.m_FilesToDeleteExt.Add(XTANDEM_DEFAULT_INPUT_FILE)
        'clsGlobal.m_FilesToDeleteExt.Add(XTANDEM_TAXONOMY_LIST_FILE)
        clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
        clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files
        clsGlobal.m_FilesToDeleteExt.Add(m_jobParams.GetParam("DatasetNum") & ".xml")

        clsGlobal.m_ExceptionFiles.Add(m_jobParams.GetParam("DatasetNum") & "_dta.zip")

        ' set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
        strErrorMessage = String.Empty
        result = UpdateParameterFile(strErrorMessage)
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesDtaRefinery.GetResources(), failed making input file: " & strErrorMessage
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function RetrieveDeconMSnLogFiles()  As Boolean

        Dim strFileNameToFind As String
        Dim SourceFolderPath As String

        Dim WorkingDir As String
        Dim DatasetName As String

        Try
            WorkingDir = m_mgrParams.GetParam("WorkDir")
            DatasetName = m_jobParams.GetParam("DatasetNum")

            strFileNameToFind = DatasetName & "_DeconMSn_log.txt"
            SourceFolderPath = FindDataFile(strFileNameToFind)

            If SourceFolderPath = "" Then
                ' Could not find the file (error will have already been logged)
                ' We'll continue on, but log a warning
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Could not find the DeconMSn Log file named " & strFileNameToFind)
                End If
            Else
                If Not CopyFileToWorkDir(strFileNameToFind, SourceFolderPath, WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & strFileNameToFind & " using folder " & SourceFolderPath)
                    End If
                    ' Ignore the error and continue
                End If
            End If


            strFileNameToFind = DatasetName & "_profile.txt"
            SourceFolderPath = FindDataFile(strFileNameToFind)

            If SourceFolderPath = "" Then
                ' Could not find the file (error will have already been logged)
                ' We'll continue on, but log a warning
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Could not find the DeconMSn Profile file named " & strFileNameToFind)
                End If
            Else
                If Not CopyFileToWorkDir(strFileNameToFind, SourceFolderPath, WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & strFileNameToFind & " using folder " & SourceFolderPath)
                    End If
                    ' Ignore the error and continue
                End If
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in clsAnalysisResourcesXT.RetrieveDtaFiles: " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Protected Function UpdateParameterFile(ByRef strErrorMessage As String) As Boolean
        'ByVal strTemplateFilePath As String, ByVal strFileToMerge As String, 
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim XTandemExePath As String
        Dim XtandemDefaultInput As String = System.IO.Path.Combine(WorkingDir, XTANDEM_DEFAULT_INPUT_FILE)
        Dim XtandemTaxonomyList As String = System.IO.Path.Combine(WorkingDir, XTANDEM_TAXONOMY_LIST_FILE)
        Dim ParamFilePath As String = System.IO.Path.Combine(WorkingDir, m_jobParams.GetParam("DTARefineryXMLFile"))
        Dim DtaRefineryDirectory As String = System.IO.Path.GetDirectoryName(m_mgrParams.GetParam("dtarefineryloc"))

        Dim SearchSettings As String = System.IO.Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("generatedFastaName"))

        Dim result As Boolean = True
        Dim fiTemplateFile As System.IO.FileInfo
        Dim objTemplate As System.Xml.XmlDocument
        strErrorMessage = String.Empty

        Try
            fiTemplateFile = New System.IO.FileInfo(ParamFilePath)

            If Not fiTemplateFile.Exists Then
                strErrorMessage = "File not found: " & fiTemplateFile.FullName
                Return False
            End If

            ' Open the template XML file
            objTemplate = New System.Xml.XmlDocument
            objTemplate.PreserveWhitespace = True
            Try
                objTemplate.Load(fiTemplateFile.FullName)
            Catch ex As Exception
                strErrorMessage = "Error loading file " & fiTemplateFile.Name & ": " & ex.Message
                Return False
            End Try

            ' Now override the values for xtandem parameters file
            Try
                Dim par As System.Xml.XmlNode
                Dim root As System.Xml.XmlElement = objTemplate.DocumentElement

                XTandemExePath = System.IO.Path.Combine(DtaRefineryDirectory, "aux_xtandem_module\tandem_5digit_precision.exe")
                par = root.SelectSingleNode("/allPars/xtandemPars/par[@label='xtandem exe file']")
                par.InnerXml = XTandemExePath

                par = root.SelectSingleNode("/allPars/xtandemPars/par[@label='default input']")
                par.InnerXml = XtandemDefaultInput

                par = root.SelectSingleNode("/allPars/xtandemPars/par[@label='taxonomy list']")
                par.InnerXml = XtandemTaxonomyList

            Catch ex As Exception
                strErrorMessage = "Error updating the MSInFile nodes: " & ex.Message
                Return False
            End Try

            ' Write out the new file
            objTemplate.Save(ParamFilePath)

        Catch ex As Exception
            strErrorMessage = "Error: " & ex.Message
            Return False
        End Try

        Return True

    End Function

End Class
