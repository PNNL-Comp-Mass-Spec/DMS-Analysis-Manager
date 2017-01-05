'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/12/2015
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMODPlus
    Inherits clsAnalysisResources

    Friend Const MOD_PLUS_RUNTIME_PARAM_FASTA_FILE_IS_DECOY As String = "###_MODPlus_Runtime_Param_FastaFileIsDecoy_###"
    Friend Const MINIMUM_PERCENT_DECOY = 25

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim currentTask = "Initializing"

        Try

            currentTask = "Retrieve shared resources"
    
            ' Retrieve shared resources, including the JobParameters file from the previous job step
            Dim result = GetSharedResources()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            currentTask = "Retrieve Fasta and param file"
            If Not RetrieveFastaAndParamFile() Then
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            currentTask = "Get Input file"

            Dim eResult As IJobParams.CloseOutType
            eResult = GetMsXmlFile()

            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return eResult
            End If
            
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = "Exception in GetResources: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

    Private Function RetrieveFastaAndParamFile() As Boolean

        Dim currentTask = "Initializing"

        Try
            Dim proteinCollections = m_jobParams.GetParam("ProteinCollectionList", String.Empty)
            Dim proteinOptions = m_jobParams.GetParam("ProteinOptions", String.Empty)

            If String.IsNullOrEmpty(proteinCollections) Then
                LogError("Job parameter ProteinCollectionList not found; unable to check for decoy fasta file")
                Return False
            End If

            If String.IsNullOrEmpty(proteinOptions) Then
                LogError("Job parameter ProteinOptions not found; unable to check for decoy fasta file")
                Return False
            End If

            Dim checkLegacyFastaForDecoy = False

            If clsGlobal.IsMatch(proteinCollections, "na") Then
                ' Legacy fasta file
                ' Need to open it with a reader and look for entries that start with XXX.
                checkLegacyFastaForDecoy = True
            Else
                If Not proteinOptions.ToLower().Contains("seq_direction=decoy") Then
                    LogError("Job parameter ProteinOptions does not contain seq_direction=decoy; cannot analyze with MODPlus; choose a DMS-generated decoy protein collection")
                    Return False
                End If
            End If
           
            ' Retrieve the Fasta file
            Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

            currentTask = "RetrieveOrgDB to " & localOrgDbFolder

            If Not RetrieveOrgDB(localOrgDbFolder) Then Return False

            If checkLegacyFastaForDecoy Then
                If Not FastaHasDecoyProteins() Then
                    Return False
                End If
            End If

            m_jobParams.AddAdditionalParameter("MODPlus", MOD_PLUS_RUNTIME_PARAM_FASTA_FILE_IS_DECOY, "True")

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

    Private Function FastaHasDecoyProteins() As Boolean

        Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

        Dim fiFastaFile = New FileInfo(fastaFilePath)

        If Not fiFastaFile.Exists Then
            ' Fasta file not found
            LogError("Fasta file not found: " & fiFastaFile.Name, "Fasta file not found: " & fiFastaFile.FullName)
            Return False
        End If

        ' Determine the fraction of the proteins that start with Reversed_ or XXX_ or XXX.
        Dim decoyPrefixes = GetDefaultDecoyPrefixes()
        Dim maxPercentReverse As Double = 0

        For Each decoyPrefix In decoyPrefixes

            Dim proteinCount As Integer
            Dim fractionDecoy = GetDecoyFastaCompositionStats(fiFastaFile, decoyPrefix, proteinCount)
          
            If proteinCount = 0 Then
                LogError("No proteins found in " & fiFastaFile.Name)
                Return False
            End If

            Dim percentReverse = fractionDecoy * 100

            If percentReverse >= MINIMUM_PERCENT_DECOY Then
                ' At least 25% of the proteins in the FASTA file are reverse proteins
                Return True
            End If

            If percentReverse > maxPercentReverse Then
                maxPercentReverse = percentReverse
            End If
        Next

        Dim addonMsg = "choose a DMS-generated decoy protein collection or a legacy fasta file with protein names that start with " & String.Join(" or ", decoyPrefixes)

        If Math.Abs(maxPercentReverse - 0) < Single.Epsilon Then
            LogError("Legacy fasta file " & fiFastaFile.Name & " does not have any decoy (reverse) proteins; " & addonMsg)
            Return False
        End If

        LogError("Fewer than " & MINIMUM_PERCENT_DECOY & "% of the proteins in legacy fasta file " & fiFastaFile.Name & " are decoy (reverse) proteins (" & maxPercentReverse.ToString("0") & "%); " & addonMsg)
        Return False

    End Function
End Class
