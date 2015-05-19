'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads a msgfdb_syn.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports PHRPReader

Public Class clsMSGFInputCreatorMSGFDB
	Inherits clsMSGFInputCreator

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="strDatasetName">Dataset name</param>
	''' <param name="strWorkDir">Working directory</param>
	''' <remarks></remarks>
	Public Sub New(ByVal strDatasetName As String, ByVal strWorkDir As String)

        MyBase.New(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.MSGFDB)

    End Sub

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for MSGFDB synopsis files
        mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserMSGFDB.GetPHRPFirstHitsFileName(mDatasetName))
        mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserMSGFDB.GetPHRPSynopsisFileName(mDatasetName))

    End Sub

    ''' <summary>
    ''' Reads a MODa or MODPlus FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
    ''' using the Probability values for the MSGF score
    ''' </summary>
    ''' <param name="strSourceFilePath"></param>
    ''' <param name="strSourceFileDescription"></param>
    ''' <returns></returns>
    ''' <remarks>Note that higher probability values are better.  Also, note that Probability is actually just a score between 0 and 1; not a true probability</remarks>
    <Obsolete("This function does not appear to be used anywhere")>
    Public Function CreateMSGFFileUsingMODaOrModPlusProbabilities(
       strSourceFilePath As String,
       eResultType As clsPHRPReader.ePeptideHitResultType,
       strSourceFileDescription As String) As Boolean

        Dim strMSGFFilePath As String

        Try

            If String.IsNullOrEmpty(strSourceFilePath) Then
                ' Source file not defined
                mErrorMessage = "Source file not provided to CreateMSGFFileUsingMODaOrModPlusProbabilities"
                Console.WriteLine(mErrorMessage)
                Return False
            End If

            Dim startupOptions As clsPHRPStartupOptions = GetMinimalMemoryPHRPStartupOptions()

            Dim probabilityColumnName = clsPHRPParserMODPlus.DATA_COLUMN_Probability

            If eResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
                probabilityColumnName = clsPHRPParserMODa.DATA_COLUMN_Probability
            End If

            ' Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
            Using objReader = New clsPHRPReader(strSourceFilePath, eResultType, startupOptions)
                objReader.SkipDuplicatePSMs = False

                ' Define the path to write the first-hits MSGF results to
                strMSGFFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(strSourceFilePath) & MSGF_RESULT_FILENAME_SUFFIX)

                ' Create the output file
                Using swMSGFFile = New StreamWriter(New FileStream(strMSGFFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                    ' Write out the headers to swMSGFFHTFile
                    WriteMSGFResultsHeaders(swMSGFFile)

                    Do While objReader.MoveNext()

                        Dim objPSM = objReader.CurrentPSM

                        ' Converting MODa/MODPlus probability to a fake Spectral Probability using 1 - probability
                        Dim dblProbability = objPSM.GetScoreDbl(probabilityColumnName, 0)
                        Dim strProbabilityValue = (1 - dblProbability).ToString("0.0000")

                        ' objPSM.MSGFSpecProb comes from column Probability
                        swMSGFFile.WriteLine(
                           objPSM.ResultID & ControlChars.Tab &
                           objPSM.ScanNumber & ControlChars.Tab &
                           objPSM.Charge & ControlChars.Tab &
                           objPSM.ProteinFirst & ControlChars.Tab &
                           objPSM.Peptide & ControlChars.Tab &
                           strProbabilityValue & ControlChars.Tab &
                           String.Empty)
                    Loop

                End Using

            End Using

        Catch ex As Exception
            ReportError("Error creating the MSGF file for the MODa / MODPlus file " & Path.GetFileName(strSourceFilePath) & ": " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Reads a MSGFDB FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
    ''' using the MSGFDB_SpecProb values for the MSGF score
    ''' </summary>
    ''' <param name="strSourceFilePath"></param>
    ''' <param name="strSourceFileDescription"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function CreateMSGFFileUsingMSGFDBSpecProb(ByVal strSourceFilePath As String, strSourceFileDescription As String) As Boolean

        Dim strMSGFFilePath As String

        Try

            If String.IsNullOrEmpty(strSourceFilePath) Then
                ' Source file not defined
                mErrorMessage = "Source file not provided to CreateMSGFFileUsingMSGFDBSpecProb"
                Console.WriteLine(mErrorMessage)
                Return False
            End If

            Dim startupOptions As clsPHRPStartupOptions = GetMinimalMemoryPHRPStartupOptions()

            ' Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
            Using objReader = New clsPHRPReader(strSourceFilePath, clsPHRPReader.ePeptideHitResultType.MSGFDB, startupOptions)
                objReader.SkipDuplicatePSMs = False

                ' Define the path to write the first-hits MSGF results to
                strMSGFFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(strSourceFilePath) & MSGF_RESULT_FILENAME_SUFFIX)

                ' Create the output file
                Using swMSGFFile As StreamWriter = New StreamWriter(New FileStream(strMSGFFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                    ' Write out the headers to swMSGFFHTFile
                    WriteMSGFResultsHeaders(swMSGFFile)

                    Do While objReader.MoveNext()

                        Dim objPSM = objReader.CurrentPSM

                        ' objPSM.MSGFSpecProb comes from column MSGFDB_SpecProb   if MS-GFDB
                        '                 it  comes from column MSGFDB_SpecEValue if MS-GF+
                        swMSGFFile.WriteLine( _
                           objPSM.ResultID & ControlChars.Tab & _
                           objPSM.ScanNumber & ControlChars.Tab & _
                           objPSM.Charge & ControlChars.Tab & _
                           objPSM.ProteinFirst & ControlChars.Tab & _
                           objPSM.Peptide & ControlChars.Tab & _
                           objPSM.MSGFSpecProb & ControlChars.Tab & _
                           String.Empty)
                    Loop

                End Using

            End Using

        Catch ex As Exception
            ReportError("Error creating the MSGF file for MSGFDB file " & Path.GetFileName(strSourceFilePath) & ": " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Protected Overrides Function PassesFilters(ByRef objPSM As PHRPReader.clsPSM) As Boolean
        Dim blnPassesFilters As Boolean

        ' All MSGFDB data is considered to be "filter-passing"
        blnPassesFilters = True

        Return blnPassesFilters

    End Function

End Class
