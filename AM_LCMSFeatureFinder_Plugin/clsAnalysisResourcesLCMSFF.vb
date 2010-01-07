Option Strict On

'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesLCMSFF
    Inherits clsAnalysisResources

    Public Const SCANS_FILE_SUFFIX As String = "_scans.csv"
    Public Const ISOS_FILE_SUFFIX As String = "_isos.csv"

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim result As Boolean
        Dim strFileToGet As String
        Dim strLCMSFFIniFileName As String
        Dim strFFIniFileStoragePath As String
        Dim strParamFileStoragePathKeyName As String

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        'Retrieve Decon2LS _isos.csv and _scans.csv files for this dataset
        strFileToGet = m_jobParams.GetParam("DatasetNum") & ISOS_FILE_SUFFIX
        If Not FindAndRetrieveMiscFiles(strFileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_XT_FILES
        End If
        clsGlobal.FilesToDelete.Add(strFileToGet)

        strFileToGet = m_jobParams.GetParam("DatasetNum") & SCANS_FILE_SUFFIX
        If Not FindAndRetrieveMiscFiles(strFileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_XT_FILES
        End If
        clsGlobal.FilesToDelete.Add(strFileToGet)

        ' Retrieve the LCMSFeatureFinder .Ini file specified for this job

        strLCMSFFIniFileName = m_jobParams.GetParam("LCMSFeatureFinderIniFile")
        If strLCMSFFIniFileName Is Nothing OrElse strLCMSFFIniFileName.Length = 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "LCMSFeatureFinderIniFile not defined in the settings for this job; unable to continue")
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        strParamFileStoragePathKeyName = AnalysisManagerBase.clsAnalysisMgrSettings.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "LCMSFeatureFinder"
        strFFIniFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
        If strFFIniFileStoragePath Is Nothing OrElse strFFIniFileStoragePath.Length = 0 Then
            strFFIniFileStoragePath = "\\gigasax\DMS_Parameter_Files\LCMSFeatureFinder"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strFFIniFileStoragePath)
        End If

        If Not CopyFileToWorkDir(strLCMSFFIniFileName, strFFIniFileStoragePath, m_WorkingDir) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_XT_FILES
        End If

        ' Could add an extension of a file to delete, like this:
        ''clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files
        ''
        ''Dim ext As String
        ''Dim DumFiles() As String

        ' ''update list of files to be deleted after run
        ''For Each ext In clsGlobal.m_FilesToDeleteExt
        ''    DumFiles = Directory.GetFiles(m_mgrParams.GetParam("workdir"), "*" & ext) 'Zipped DTA
        ''    For Each FileToDel As String In DumFiles
        ''        clsGlobal.FilesToDelete.Add(FileToDel)
        ''    Next
        ''Next

        ' Customize the LCMSFeatureFinder .Ini file to include the input file path and output folder path
        result = UpdateFeatureFinderIniFile(strLCMSFFIniFileName)
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesLCMSFF.GetResources(), failed customizing .Ini file " & strLCMSFFIniFileName
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function UpdateFeatureFinderIniFile(ByVal strLCMSFFIniFileName As String) As Boolean

        Const INPUT_FILENAME_KEY As String = "InputFileName"
        Const OUTPUT_DIRECTORY_KEY As String = "OutputDirectory"

        Dim result As Boolean = True

        ' Read the source .Ini file and update the settings for InputFileName and OutputDirectory

        Dim srInFile As System.IO.StreamReader
        Dim swOutFile As System.IO.StreamWriter

        Dim SrcFilePath As String = System.IO.Path.Combine(m_WorkingDir, strLCMSFFIniFileName)
        Dim TargetFilePath As String = System.IO.Path.Combine(m_WorkingDir, strLCMSFFIniFileName & "_new")
        Dim IsosFilePath As String = System.IO.Path.Combine(m_WorkingDir, m_jobParams.GetParam("DatasetNum") & ISOS_FILE_SUFFIX)

        Dim strLineIn As String
        Dim strLineInLCase As String

        Dim blnInputFileDefined As Boolean
        Dim blnOutputDirectoryDefined As Boolean

        blnInputFileDefined = False
        blnOutputDirectoryDefined = False
        result = True

        ' Open the intput file
        Try
            srInFile = New System.IO.StreamReader(New System.IO.FileStream(SrcFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

            ' Create the output file (temporary name ending in "_new"; we'll swap the files later)
            Try

                swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(TargetFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

                Do While srInFile.Peek >= 0
                    strLineIn = srInFile.ReadLine

                    If Not strLineIn Is Nothing Then
                        strLineInLCase = strLineIn.ToLower.Trim

                        If strLineInLCase.StartsWith(INPUT_FILENAME_KEY.ToLower) Then
                            ' Customize the input file name
                            strLineIn = INPUT_FILENAME_KEY & "=" & IsosFilePath
                            blnInputFileDefined = True
                        End If

                        If strLineInLCase.StartsWith(OUTPUT_DIRECTORY_KEY.ToLower) Then
                            ' Customize the output directory name
                            strLineIn = OUTPUT_DIRECTORY_KEY & "=" & m_WorkingDir
                            blnOutputDirectoryDefined = True
                        End If

                        swOutFile.WriteLine(strLineIn)
                    End If
                Loop

                If Not blnInputFileDefined Then
                    swOutFile.WriteLine(INPUT_FILENAME_KEY & "=" & IsosFilePath)
                End If

                If Not blnOutputDirectoryDefined Then
                    swOutFile.WriteLine(OUTPUT_DIRECTORY_KEY & "=" & m_WorkingDir)
                End If

                swOutFile.Close()
                srInFile.Close()

            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize (" & strLCMSFFIniFileName & "): " & ex.Message)
                result = False

            End Try

            ' Wait 250 millseconds, then replace the original .Ini file with the new one
            System.Threading.Thread.Sleep(250)
            GC.Collect()
            GC.WaitForPendingFinalizers()

            ' Delete the input file
            System.IO.File.Delete(SrcFilePath)

            ' Wait another 250 milliseconds before renaming the output file
            System.Threading.Thread.Sleep(50)
            GC.Collect()
            GC.WaitForPendingFinalizers()

            ' Rename the newly created output file to have the name of the input file
            System.IO.File.Move(TargetFilePath, SrcFilePath)

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize (" & strLCMSFFIniFileName & "): " & ex.Message)
            result = False
        End Try

        Return result
    End Function

End Class
