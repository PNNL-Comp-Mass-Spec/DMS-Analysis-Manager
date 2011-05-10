
'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/10/2011
'
' Uses MSConvert to create a .mzXML or .mzML file
'*********************************************************************************************************

Option Strict On

Public Class clsMSXmlGenMSConvert
    Inherits clsMSXmlGen

#Region "Methods"

    Public Sub New(ByVal WorkDir As String, _
                   ByVal MSConvertProgramPath As String, _
                   ByVal DatasetName As String, _
                   ByVal eOutputType As MSXMLOutputTypeConstants, _
                   ByVal CentroidMSXML As Boolean)

        MyBase.New(WorkDir, MSConvertProgramPath, DatasetName, eOutputType, CentroidMSXML)

        mUseProgRunnerResultCode = False

    End Sub

    
    Protected Overrides Function CreateArguments(ByVal msXmlFormat As String, ByVal RawFilePath As String) As String

        Dim CmdStr As String

        If mCentroidMSXML Then
            ' Note: MSConvert does not support centroiding
        End If

        CmdStr = " " & RawFilePath & " --" & msXmlFormat & " -o " & mWorkDir

        Return CmdStr
    End Function

    Protected Overrides Function SetupTool() As Boolean

        Try
            ' Create a registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
            ' to indicate that we agree to the Thermo license

            Dim regSoftware As Microsoft.Win32.RegistryKey
            Dim regProteoWizard As Microsoft.Win32.RegistryKey
            Dim blnSubKeyMissing As Boolean
            Dim blnValueMissing As Boolean

            Try

                If mDebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Confirming that 'Software\ProteoWizard' registry key exists")
                End If

                regSoftware = Microsoft.Win32.Registry.CurrentUser.OpenSubKey("Software", False)
                regProteoWizard = regSoftware.OpenSubKey("ProteoWizard", False)

                If regProteoWizard Is Nothing Then
                    blnSubKeyMissing = True
                Else
                    blnSubKeyMissing = False

                    Dim objValue As Object

                    If mDebugLevel >= 2 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Confirming that 'Thermo MSFileReader' registry key exists")
                    End If

                    objValue = regProteoWizard.GetValue("Thermo MSFileReader")

                    If objValue Is Nothing Then
                        blnValueMissing = True
                    ElseIf String.IsNullOrEmpty(CStr(objValue)) Then
                        blnValueMissing = True
                    Else
                        If Boolean.Parse(CStr(objValue)) = True Then
                            blnValueMissing = False
                        Else
                            blnValueMissing = True
                        End If
                    End If

                    regProteoWizard.Close()
                    End If

                    regSoftware.Close()

            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Exception looking for key (possibly not found): " & ex.Message)
                blnSubKeyMissing = True
                blnValueMissing = True
            End Try

            If blnSubKeyMissing Or blnValueMissing Then
                regSoftware = Microsoft.Win32.Registry.CurrentUser.OpenSubKey("Software", True)

                If blnSubKeyMissing Then
                    If mDebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating 'Software\ProteoWizard' SubKey")
                    End If
                    regProteoWizard = regSoftware.CreateSubKey("ProteoWizard")
                Else
                    If mDebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Opening 'Software\ProteoWizard' SubKey")
                    End If
                    regProteoWizard = regSoftware.OpenSubKey("ProteoWizard", True)
                End If

                If mDebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Setting value for 'Thermo MSFileReader' registry key to 'True'")
                End If

                regProteoWizard.SetValue("Thermo MSFileReader", "True")
                regProteoWizard.Close()
                regSoftware.Close()
            End If

        Catch ex As Exception
            mErrorMessage = "Error creating ProteoWizard registry key: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage)
            Return False
        End Try

        Return True
    End Function

#End Region

End Class
