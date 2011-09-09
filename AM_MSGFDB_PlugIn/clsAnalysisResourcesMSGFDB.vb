'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSGFDB
    Inherits clsAnalysisResources

    Private WithEvents mCDTACondenser As CondenseCDTAFile.clsCDTAFileCondenser

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim eResult As IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        ' Make sure the machine has enough free memory to run MSGFDB
        eResult = ValidateFreeMemorySize()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        ' Retrieve param file
        ' This will also obtain the _ModDefs.txt file using query 
        '  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
        '  FROM V_Param_File_Mass_Mod_Info 
        '  WHERE Param_File_Name = 'ParamFileName'
        If Not RetrieveGeneratedParamFile( _
           m_jobParams.GetParam("ParmFileName"), _
           m_jobParams.GetParam("ParmFileStoragePath"), _
           m_mgrParams.GetParam("workdir")) _
        Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Retrieve unzipped dta files (do not de-concatenate since MSGFDB uses the _Dta.txt file directly)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
        clsGlobal.m_FilesToDeleteExt.Add("temp.tsv") ' MSGFDB creates .txt.temp.tsv files, which we don't need

        ' If the _dta.txt file is over 2 GB in size, then condense it

        If Not ValidateDTATextFileSize(m_WorkingDir, m_jobParams.GetParam("datasetNum") & "_dta.txt") Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function ValidateDTATextFileSize(ByVal strWorkDir As String, ByVal strInputFileName As String) As Boolean
        Const FILE_SIZE_THRESHOLD As Integer = Int32.MaxValue

        Dim ioFileInfo As System.IO.FileInfo
        Dim strInputFilePath As String
        Dim strFilePathOld As String

        Dim strMessage As String

        Dim blnSuccess As Boolean

        Try
            strInputFilePath = System.IO.Path.Combine(strWorkDir, strInputFileName)
            ioFileInfo = New System.IO.FileInfo(strInputFilePath)

            If Not ioFileInfo.Exists Then
                m_message = "_DTA.txt file not found: " & strInputFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            If ioFileInfo.Length >= FILE_SIZE_THRESHOLD Then
                ' Need to condense the file

                strMessage = ioFileInfo.Name & " is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB in size; will now condense it by combining data points with consecutive zero-intensity values"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)

                mCDTACondenser = New CondenseCDTAFile.clsCDTAFileCondenser

                blnSuccess = mCDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName)

                If Not blnSuccess Then
                    m_message = "Error condensing _DTA.txt file: " & mCDTACondenser.GetErrorMessage()
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                Else
                    ' Wait 500 msec, then check the size of the new _dta.txt file
                    System.Threading.Thread.Sleep(500)

                    ioFileInfo.Refresh()

                    If m_DebugLevel >= 1 Then
                        strMessage = "Condensing complete; size of the new _dta.txt file is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                    End If

                    Try
                        strFilePathOld = System.IO.Path.Combine(strWorkDir, System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.FullName) & "_Old.txt")

                        If m_DebugLevel >= 2 Then
                            strMessage = "Now deleting file " & strFilePathOld
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                        End If

                        ioFileInfo = New System.IO.FileInfo(strFilePathOld)
                        If ioFileInfo.Exists Then
                            ioFileInfo.Delete()
                        Else
                            strMessage = "Old _DTA.txt file not found:" & ioFileInfo.FullName & "; cannot delete"
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
                        End If

                    Catch ex As Exception
                        ' Error deleting the file; log it but keep processing
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception deleting _dta_old.txt file: " & ex.Message)
                    End Try

                End If
            End If

            blnSuccess = True

        Catch ex As Exception
            m_message = "Exception in ValidateDTATextFileSize"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Lookups the amount of memory that will be reserved for Java
    ''' If this value is >= the free memory, then returns CLOSEOUT_FAILED
    ''' Otherwise, returns CLOSEOUT_SUCCESS
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ValidateFreeMemorySize() As IJobParams.CloseOutType

        ' Note: This function is also present in clsAnalysisToolRunnerMSGFDB
        '       but that function posts a log entry if sufficient memory _is_ available
        ' This function only posts a log entry if not enough memory is available

        Dim intJavaMemorySizeMB As Integer
        Dim sngFreeMemoryMB As Single
        Dim strMessage As String

        intJavaMemorySizeMB = clsGlobal.GetJobParameter(m_jobParams, "MSGFDBJavaMemorySize", 2000)
        If intJavaMemorySizeMB < 512 Then intJavaMemorySizeMB = 512

        sngFreeMemoryMB = clsCreateMSGFDBSuffixArrayFiles.GetFreeMemoryMB()

        If intJavaMemorySizeMB >= sngFreeMemoryMB Then
            m_message = "Not enough free memory to run MSGFDB; need " & intJavaMemorySizeMB & " MB"

            strMessage = m_message & " but system has " & sngFreeMemoryMB.ToString("0") & " MB available"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If
    End Function

    Private Sub mCDTACondenser_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mCDTACondenser.ProgressChanged
        Static dtLastUpdateTime As System.DateTime

        If m_DebugLevel >= 1 Then
            If m_DebugLevel = 1 AndAlso System.DateTime.Now.Subtract(dtLastUpdateTime).TotalSeconds >= 60 OrElse _
               m_DebugLevel > 1 AndAlso System.DateTime.Now.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
                dtLastUpdateTime = System.DateTime.Now

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & percentComplete.ToString("0.00") & "% complete")
            End If
        End If
    End Sub

End Class
