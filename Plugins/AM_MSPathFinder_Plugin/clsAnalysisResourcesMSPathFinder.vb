'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/15/2014
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSPathFinder
    Inherits clsAnalysisResources

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        If Not RetrieveFastaAndParamFile() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If
        Dim eResult As IJobParams.CloseOutType

        eResult = RetrieveProMexFeaturesFile()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return eResult
        End If

        eResult = RetrievePBFFile()

        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return eResult
        End If

        ' Look for existing .tsv result files
        ' These typically will not exist, but may exist if a search was interrupted before it finished
        If Not RetrieveExistingSearchResults() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Look for existing .tsv result files
    ''' These will only exist if a search was interrupted before it finished
    ''' The files will be in a subfolder below DMS_FailedResults and will need to have been manually copied to the transfer folder for this job
    ''' </summary>
    ''' <returns>True if success (even if no files were found); false if an error</returns>
    Private Function RetrieveExistingSearchResults() As Boolean

        Dim fileSuffixes = New List(Of String) From {"_IcDecoy.tsv", "_IcTarget.tsv", "_IcTda.tsv"}

        Try
            Dim transferFolderPathBase = m_jobParams.GetParam("transferFolderPath")
            If String.IsNullOrEmpty(transferFolderPathBase) Then
                ' Transfer folder parameter is empty; abort the search for result files 
                ' This error will be properly dealt with elsewhere
                Return False
            End If

            ' Append the dataset folder name to the transfer folder path
            Dim datasetFolderName = m_jobParams.GetParam("StepParameters", "DatasetFolderName")
            If String.IsNullOrWhiteSpace(datasetFolderName) Then datasetFolderName = m_DatasetName

            Dim resultFolderName = m_jobParams.GetParam("OutputFolderName")
            If String.IsNullOrEmpty(resultFolderName) Then
                'Output folder parameter is empty; abort the search for result files 
                Return False
            End If

            Dim transferFolderPath = Path.Combine(transferFolderPathBase, datasetFolderName, resultFolderName)

            For Each suffix In fileSuffixes

                Dim sourceFile = New FileInfo(Path.Combine(transferFolderPath, m_DatasetName & suffix))

                If Not sourceFile.Exists Then
                    ' File not found; move on to the next file
                    Continue For
                End If

                ' Copy the file
                If Not CopyFileToWorkDir(sourceFile.Name, transferFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
                    ' Error copying; move on to the next file
                    Continue For
                End If

            Next

        Catch ex As Exception
            m_message = "Exception in RetrieveExistingSearchResults: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return True

    End Function

    Private Function RetrieveFastaAndParamFile() As Boolean

        Dim currentTask = "Initializing"

        Try

            ' Retrieve the Fasta file
            Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

            currentTask = "RetrieveOrgDB to " & localOrgDbFolder

            If Not RetrieveOrgDB(localOrgDbFolder) Then Return False

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

            ' Retrieve the parameter file
            ' This will also obtain the _ModDefs.txt file using query 
            '  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
            '  FROM V_Param_File_Mass_Mod_Info 
            '  WHERE Param_File_Name = 'ParamFileName'

            Dim paramFileName = m_jobParams.GetParam("ParmFileName")

            currentTask = "RetrieveGeneratedParamFile " & paramFileName

            If Not RetrieveGeneratedParamFile(paramFileName) Then
                Return False
            End If

            Return True

        Catch ex As Exception
            m_message = "Exception in RetrieveFastaAndParamFile: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return False
        End Try

    End Function

    Private Function RetrievePBFFile() As IJobParams.CloseOutType

        Const PBF_GEN_FOLDER_PREFIX = "PBF_GEN"

        Dim currentTask = "Initializing"

        Try

            ' Cache the input folder name
            Dim inputFolderNameCached = m_jobParams.GetJobParameter("InputFolderName", String.Empty)
            Dim inputFolderNameWasUpdated = False

            If Not inputFolderNameCached.ToUpper().StartsWith(PBF_GEN_FOLDER_PREFIX) Then
                ' Update the input folder to be the PBF_Gen input folder for this job (should be the input_folder of the previous job step)
                Dim stepNum = m_jobParams.GetJobParameter("Step", 100)

                ' Gigasax.DMS_Pipeline
                Dim dmsConnectionString = m_mgrParams.GetParam("brokerconnectionstring")

                Dim sql = " SELECT Input_Folder_Name " &
                          " FROM T_Job_Steps" &
                          " WHERE Job = " & m_JobNum & " AND Step_Number < " & stepNum & " AND Input_Folder_Name LIKE '" & PBF_GEN_FOLDER_PREFIX & "%'" &
                          " ORDER by Step_Number DESC"

                Dim lstResults As List(Of String) = Nothing

                If Not clsGlobal.GetQueryResultsTopRow(sql, dmsConnectionString, lstResults, "RetrievePBFFile") Then
                    m_message = "Error looking up the correct PBF_Gen folder name in T_Job_Steps"
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                Dim pbfGenFolderName = lstResults.FirstOrDefault()

                If String.IsNullOrWhiteSpace(pbfGenFolderName) Then
                    m_message = "PBF_Gen folder name listed in T_Job_Steps for step " & stepNum - 1 & " was empty"
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                m_jobParams.SetParam("InputFolderName", pbfGenFolderName)
                inputFolderNameWasUpdated = True
            End If

            ' Retrieve the .pbf file from the MSXml cache folder

            currentTask = "RetrievePBFFile"

            Dim eResult = GetPBFFile()

            If inputFolderNameWasUpdated Then
                m_jobParams.SetParam("InputFolderName", inputFolderNameCached)
            End If

            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return eResult
            End If

            m_jobParams.AddResultFileExtensionToSkip(DOT_PBF_EXTENSION)

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = "Exception in RetrievePBFFile: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

    Private Function RetrieveProMexFeaturesFile() As IJobParams.CloseOutType

        Try
            Dim fileToGet = m_DatasetName & DOT_MS1FT_EXTENSION

            If Not FindAndRetrieveMiscFiles(fileToGet, False) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            m_jobParams.AddResultFileExtensionToSkip(DOT_MS1FT_EXTENSION)

        Catch ex As Exception
            m_message = "Exception in RetrieveProMexFeaturesFile: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function
End Class
