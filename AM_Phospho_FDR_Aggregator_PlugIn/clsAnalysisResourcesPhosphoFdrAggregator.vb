Option Strict On

Imports AnalysisManagerBase
Imports System.Xml.XPath
Imports System.Xml
Imports System.Collections.Specialized

Public Class clsAnalysisResourcesPhosphoFdrAggregator
    Inherits clsAnalysisResources

    Friend Const ASCORE_INPUT_FILE As String = "AScoreBatch.xml"
    Protected WithEvents CmdRunner As clsRunDosProgram

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        Dim SplitString As String()
        Dim FileNameExt As String()

        SplitString = m_jobParams.GetParam("TargetJobFileList").Split(","c)
        For Each row As String In SplitString
            FileNameExt = row.Split(":"c)
            If FileNameExt(2) = "nocopy" Then
                clsGlobal.m_FilesToDeleteExt.Add(FileNameExt(1))
            End If
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")
        If Not String.IsNullOrEmpty(m_jobParams.GetParam("AScoreCIDParamFile")) Then
            If Not RetrieveFile(m_jobParams.GetParam("AScoreCIDParamFile"), _
                                m_jobParams.GetParam("transferFolderPath"), _
                                m_mgrParams.GetParam("workdir")) _
            Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        If Not String.IsNullOrEmpty(m_jobParams.GetParam("AScoreETDParamFile")) Then
            If Not RetrieveFile(m_jobParams.GetParam("AScoreETDParamFile"), _
                                m_jobParams.GetParam("transferFolderPath"), _
                                m_mgrParams.GetParam("workdir")) _
            Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        If Not String.IsNullOrEmpty(m_jobParams.GetParam("AScoreHCDParamFile")) Then
            If Not RetrieveFile(m_jobParams.GetParam("AScoreHCDParamFile"), _
                                m_jobParams.GetParam("transferFolderPath"), _
                                m_mgrParams.GetParam("workdir")) _
            Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files")

        If Not RetrieveAggregateFiles(SplitString) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Building AScore input file")

        If Not BuildInputFile() Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function BuildInputFile() As Boolean
        Dim DatasetFiles() As String
        Dim DatasetType As String
        Dim DatasetName As String
        Dim DatasetID As String
        Dim WorkDir As String = m_mgrParams.GetParam("workdir")
        Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(WorkDir, ASCORE_INPUT_FILE))

        Try
            inputFile.WriteLine("<?xml version=""1.0"" encoding=""UTF-8"" ?>")
            inputFile.WriteLine("<ascore_batch>")
            inputFile.WriteLine("  <settings>")
            inputFile.WriteLine("    <max_threads>4</max_threads>")
            inputFile.WriteLine("  </settings>")

            'update list of files to be deleted after run
            DatasetFiles = System.IO.Directory.GetFiles(WorkDir, "*_syn*.txt")
            For Each Dataset As String In DatasetFiles
                Dataset = System.IO.Path.GetFileName(Dataset)

                ' Function RetrieveAggregateFilesRename in clsAnalysisResources in the main analysis manager program
                '  will have appended _hcd, _etd, or _cid to the synopsis dta, fht, and syn file for each dataset
                '  The suffix to use is based on text present in the settings file name for each job
                ' However, if the settings file name did not contain HCD, ETD, or CID, then the dta, fht, and syn files
                '  will not have had a suffix added; in that case, DatasetType will be ".txt"
                DatasetType = Dataset.Substring(Dataset.ToLower().IndexOf("_syn") + 4, 4)

                ' If DatasetType is ".txt" then change it to an empty string
                If DatasetType.ToLower() = ".txt" Then DatasetType = String.Empty

                DatasetName = Dataset.Substring(0, Dataset.Length - (Dataset.Length - Dataset.ToLower().IndexOf("_syn")))
                inputFile.WriteLine("  <run>")

                DatasetID = GetDatasetID(DatasetName)

                If String.IsNullOrEmpty(DatasetType) OrElse DatasetType = "_cid" Then
                    inputFile.WriteLine("    <param_file>" & System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreCIDParamFile")) & "</param_file>")
                ElseIf DatasetType = "_hcd" Then
                    inputFile.WriteLine("    <param_file>" & System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreHCDParamFile")) & "</param_file>")
                ElseIf DatasetType = "_etd" Then
                    inputFile.WriteLine("    <param_file>" & System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreETDParamFile")) & "</param_file>")
                End If
                inputFile.WriteLine("    <output_path>" & WorkDir & "</output_path>")
                inputFile.WriteLine("    <dta_file>" & System.IO.Path.Combine(WorkDir, DatasetName & "_dta" & DatasetType & ".txt") & "</dta_file>")
                inputFile.WriteLine("    <fht_file>" & System.IO.Path.Combine(WorkDir, DatasetName & "_fht" & DatasetType & ".txt") & "</fht_file>")
                inputFile.WriteLine("    <syn_file>" & System.IO.Path.Combine(WorkDir, DatasetName & "_syn" & DatasetType & ".txt") & "</syn_file>")
                inputFile.WriteLine("    <scan_stats_file>" & System.IO.Path.Combine(WorkDir, DatasetName & "_ScanStatsEx" & ".txt") & "</scan_stats_file>")
                inputFile.WriteLine("    <dataset_id>" & DatasetID & "</dataset_id>")
                inputFile.WriteLine("  </run>")
            Next

            inputFile.WriteLine("</ascore_batch>")

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating AScore input file" & ex.Message)

        Finally
            inputFile.Close()
        End Try

        Return True

    End Function

    Protected Function GetDatasetID(ByVal DatasetName As String) As String
        Dim Dataset_ID As String = ""
        Dim Dataset_DatasetID As String()

        For Each Item As String In clsGlobal.m_DatasetInfoList
            Dataset_DatasetID = Item.Split(":"c)
            If Dataset_DatasetID(0) = DatasetName Then
                Return Dataset_DatasetID(1)
            End If
        Next

        Return Dataset_ID

    End Function

End Class
