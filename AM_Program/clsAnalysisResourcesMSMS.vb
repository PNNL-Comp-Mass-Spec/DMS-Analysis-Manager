'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/18/2008
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO
Imports Protein_Exporter
Imports PRISM.Logging
Imports Protein_Exporter.ExportProteinCollectionsIFC
Imports System.Timers

Namespace AnalysisManagerMSMSResourceBase

	Public Class clsAnalysisResourcesMSMS
		Inherits clsAnalysisResources

		'*********************************************************************************************************
		'Base class for retrieving Sequest and XTandem analysis resources
		'*********************************************************************************************************

#Region "Constants"
		'TODO: What the heck is this?
		Private Const FASTA_GEN_TIMEOUT_INTERVAL As Integer = 600000	 '10 minutes
#End Region

#Region "Module variables"
		Private m_ErrMsg As String = ""
		Private WithEvents m_FastaTools As ExportProteinCollectionsIFC.IGetFASTAFromDMS
		Private m_GenerationStarted As Boolean = False
		Private m_GenerationComplete As Boolean = False
		Private m_FastaToolsCnStr As String = ""
		Private m_FastaFileName As String = ""
		Private WithEvents m_FastaTimer As Timer
		Private m_FastaGenTimeOut As Boolean = False
#End Region

#Region "Properties"
		Public ReadOnly Property ErrMsg() As String
			Get
				Return m_ErrMsg
			End Get
		End Property

		Public Property FastaDbConnStr() As String
			Get
				Return m_FastaToolsCnStr
			End Get
			Set(ByVal Value As String)
				m_FastaToolsCnStr = Value
			End Set
		End Property
#End Region


#Region "Methods"

		''' <summary>
		''' Initializes class
		''' </summary>
		''' <param name="mgrParams">Object holding manager parameters</param>
		''' <param name="jobParams">Object holding analysis job parameters</param>
		''' <param name="logger">Logging object</param>
		''' <remarks></remarks>
		Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As ILogger)

			'Overrides base Setup method to allow adding fasta db location
			MyBase.Setup(mgrParams, jobParams, logger)
			m_FastaToolsCnStr = m_mgrParams.GetParam("fastacnstring")

		End Sub

		''' <summary>
		''' Overrides base class orgdb copy to use Ken's dll for creating a fasta file
		''' </summary>
		''' <param name="LocalOrgDBFolder">Folder on analysis machine where fasta files are stored</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overrides Function RetrieveOrgDB(ByVal LocalOrgDBFolder As String) As Boolean

			m_logger.PostEntry("Obtaining org db file", PRISM.Logging.ILogger.logMsgType.logNormal, True)

			'Make a new fasta file from scratch
			If Not CreateFastaFile(LocalOrgDBFolder) Then
				'There was a problem. Log entries in lower-level routines provide documentation
				Return False
			End If

			'Fasta file was successfully generated. Put the private name of the generated fastafile in the
			'	job data class for other methods to use
			If Not m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName) Then Return False

			'We got to here OK, so return
			Return True

		End Function

		''' <summary>
		''' Creates a Fasta file based on Ken's DLL
		''' </summary>
		''' <param name="DestFolder">Folder where file will be created</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Public Function CreateFastaFile(ByVal DestFolder As String) As Boolean

			Dim HashString As String

			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisResourcesMSMS.CreateFastaFile(), Creating fasta file", _
				  PRISM.Logging.ILogger.logMsgType.logDebug, True)
			End If

			'Instantiate fasta tool if not already done
			If m_FastaTools Is Nothing Then
				If m_FastaToolsCnStr = "" Then
					m_ErrMsg = "Protein database connection string not specified"
					Return False
				End If
				m_FastaTools = New clsGetFASTAFromDMS(m_FastaToolsCnStr)
			End If

			'Initialize fasta generation state variables
			m_GenerationStarted = False
			m_GenerationComplete = False

			'Set up variables for fasta creation call
			Dim LegacyFasta As String = m_jobParams.GetParam("LegacyFastaFileName")
			Dim CreationOpts As String = m_jobParams.GetParam("ProteinOptions")
			Dim CollectionList As String = m_jobParams.GetParam("ProteinCollectionList")

			'Setup a timer to prevent an infinite loop if there's a fasta generation problem
			m_FastaTimer = New Timer
			m_FastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL
			m_FastaTimer.AutoReset = False

			'Create the fasta file
			m_FastaGenTimeOut = False
			Try
				m_FastaTimer.Start()
				HashString = m_FastaTools.ExportFASTAFile(CollectionList, CreationOpts, LegacyFasta, DestFolder)
			Catch Ex As Exception
				m_logger.PostError("clsAnalysisResourcesMSMS.CreateFastaFile(), Exception generating OrgDb file", Ex, True)
				Return False
			End Try

			'Wait for fasta creation to finish
			While Not m_GenerationComplete
				System.Threading.Thread.Sleep(2000)
			End While

			If m_FastaGenTimeOut Then
				'Fasta generator hung - report error and exit
				m_logger.PostEntry("clsAnalysisResourcesMSMS.CreateFastaFile(), Timeout error while generating OrdDb file", _
				  ILogger.logMsgType.logError, True)
				Return False
			End If

			'If we got to here, everything worked OK
			Return True

		End Function

		''' <summary>
		''' Uses hashing features in Ken's dll to determine if the tested Fasta file is up to date
		''' </summary>
		''' <param name="TestFastaNamePath">Fasta file to be tested</param>
		''' <param name="RefFastaNamePath">Reference fasta file</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Public Function VerifyFastaVersion(ByVal TestFastaNamePath As String, ByVal RefFastaNamePath As String) As Boolean

			Dim TestHash As String
			Dim RefHash As String

			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisResourcesMSMS.VerifyFastaVersion: Testing file" & _
				 TestFastaNamePath, PRISM.Logging.ILogger.logMsgType.logDebug, True)
			End If

			'Instantiate fasta tool if not already done
			If m_FastaTools Is Nothing Then
				If m_FastaToolsCnStr = "" Then
					m_ErrMsg = "Protein database connection string not specified"
					Return False
				End If
				m_FastaTools = New clsGetFASTAFromDMS(m_FastaToolsCnStr)
			End If

			Try
				TestHash = m_FastaTools.GenerateFileAuthenticationHash(TestFastaNamePath)
				RefHash = m_FastaTools.GenerateFileAuthenticationHash(RefFastaNamePath)
				If TestHash = RefHash Then
					Return True
				Else
					m_ErrMsg = "Hash mismatch: File " & TestFastaNamePath & ", Hash " & TestHash & _
					 " did not match file " & RefFastaNamePath & ", hash " & RefHash
					Return False
				End If
			Catch ex As Exception
				m_ErrMsg = "Exception comparing file " & TestFastaNamePath & " to file " & RefFastaNamePath
				m_logger.PostError("Exception during fasta hash comparison", ex, True)
				Return False
			End Try

		End Function

#End Region

#Region "Event handlers"
		Private Sub m_FastaTools_FileGenerationStarted1(ByVal taskMsg As String) Handles m_FastaTools.FileGenerationStarted

			m_GenerationStarted = True
			m_FastaTimer.Start()	 'Reset the fasta generation timer

		End Sub

		Private Sub m_FastaTools_FileGenerationCompleted(ByVal FullOutputPath As String) Handles m_FastaTools.FileGenerationCompleted

			m_FastaFileName = Path.GetFileName(FullOutputPath)		'Get the name of the fasta file that was generated
			m_FastaTimer.Stop()	  'Stop the fasta generation timer so no false error occurs
			m_GenerationComplete = True		'Set the completion flag

		End Sub

		Private Sub m_FastaTools_FileGenerationProgress(ByVal statusMsg As String, ByVal fractionDone As Double) Handles m_FastaTools.FileGenerationProgress

			'Reset the fasta generation timer
			m_FastaTimer.Start()

		End Sub

		Private Sub m_FastaTimer_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles m_FastaTimer.Elapsed

			'If this event occurs, it means there was a hang during fasta generation and the manager will have to quit
			m_FastaTimer.Stop()		'Stop the timer to prevent false errors
			m_FastaGenTimeOut = True	  'Set the timeout flag so an error will be reported
			m_GenerationComplete = True		'Set the completion flag so the fasta generation wait loop will exit

		End Sub
#End Region

	End Class

End Namespace
