﻿namespace TestMultiAlignPlugIn {
    partial class Form1 {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.Test_GetMultiAlignResults = new System.Windows.Forms.Button();
            this.Test_Tool_Runner = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // Test_GetMultiAlignResults
            // 
            this.Test_GetMultiAlignResults.Location = new System.Drawing.Point(16, 12);
            this.Test_GetMultiAlignResults.Name = "Test_GetMultiAlignResults";
            this.Test_GetMultiAlignResults.Size = new System.Drawing.Size(200, 23);
            this.Test_GetMultiAlignResults.TabIndex = 0;
            this.Test_GetMultiAlignResults.Text = "Test Run MultiAlign";
            this.Test_GetMultiAlignResults.UseVisualStyleBackColor = true;
            this.Test_GetMultiAlignResults.Click += new System.EventHandler(this.Test_GetMultiAlignResults_Click);
            // 
            // Test_Tool_Runner
            // 
            this.Test_Tool_Runner.Location = new System.Drawing.Point(12, 265);
            this.Test_Tool_Runner.Name = "Test_Tool_Runner";
            this.Test_Tool_Runner.Size = new System.Drawing.Size(219, 23);
            this.Test_Tool_Runner.TabIndex = 2;
            this.Test_Tool_Runner.Text = "Test Tool Runner";
            this.Test_Tool_Runner.UseVisualStyleBackColor = true;
            this.Test_Tool_Runner.Click += new System.EventHandler(this.Test_Tool_Runner_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(275, 300);
            this.Controls.Add(this.Test_Tool_Runner);
            this.Controls.Add(this.Test_GetMultiAlignResults);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button Test_GetMultiAlignResults;
        private System.Windows.Forms.Button Test_Tool_Runner;
    }
}

