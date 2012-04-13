Option Strict On

Public Class clsProteowizardTools

	Protected mDebugLevel As Integer

	Public Sub New(ByVal DebugLvl As Integer)
		mDebugLevel = DebugLvl
	End Sub

	Public Function RegisterProteoWizard() As Boolean

		Try
			' Tool setup for MSConvert involves creating a
			'  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
			'  to indicate that we agree to the Thermo license

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
			Dim Msg As String = "Error creating ProteoWizard registry key: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return False
		End Try

		Return True
	End Function

End Class
