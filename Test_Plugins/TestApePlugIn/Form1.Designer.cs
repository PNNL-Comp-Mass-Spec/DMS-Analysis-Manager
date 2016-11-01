namespace TestApePlugIn {
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
            this.Test_GetImprovResults = new System.Windows.Forms.Button();
            this.Test_RunWorkflow = new System.Windows.Forms.Button();
            this.Test_Tool_Runner = new System.Windows.Forms.Button();
            this.Test_GetQRollupResults = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // Test_GetImprovResults
            // 
            this.Test_GetImprovResults.Location = new System.Drawing.Point(16, 12);
            this.Test_GetImprovResults.Name = "Test_GetImprovResults";
            this.Test_GetImprovResults.Size = new System.Drawing.Size(200, 23);
            this.Test_GetImprovResults.TabIndex = 0;
            this.Test_GetImprovResults.Text = "Test GetImprovResults";
            this.Test_GetImprovResults.UseVisualStyleBackColor = true;
            this.Test_GetImprovResults.Click += new System.EventHandler(this.Test_GetImprovResults_Click);
            // 
            // Test_RunWorkflow
            // 
            this.Test_RunWorkflow.Location = new System.Drawing.Point(16, 41);
            this.Test_RunWorkflow.Name = "Test_RunWorkflow";
            this.Test_RunWorkflow.Size = new System.Drawing.Size(200, 23);
            this.Test_RunWorkflow.TabIndex = 1;
            this.Test_RunWorkflow.Text = "Test RunWorkflow";
            this.Test_RunWorkflow.UseVisualStyleBackColor = true;
            this.Test_RunWorkflow.Click += new System.EventHandler(this.Test_RunWorkflow_Click);
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
            // Test_GetQRollupResults
            // 
            this.Test_GetQRollupResults.Location = new System.Drawing.Point(16, 70);
            this.Test_GetQRollupResults.Name = "Test_GetQRollupResults";
            this.Test_GetQRollupResults.Size = new System.Drawing.Size(200, 23);
            this.Test_GetQRollupResults.TabIndex = 3;
            this.Test_GetQRollupResults.Text = "Test Get QRollup Results";
            this.Test_GetQRollupResults.UseVisualStyleBackColor = true;
            this.Test_GetQRollupResults.Click += new System.EventHandler(this.Test_GetQRollupResults_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(275, 300);
            this.Controls.Add(this.Test_GetQRollupResults);
            this.Controls.Add(this.Test_Tool_Runner);
            this.Controls.Add(this.Test_RunWorkflow);
            this.Controls.Add(this.Test_GetImprovResults);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button Test_GetImprovResults;
        private System.Windows.Forms.Button Test_RunWorkflow;
        private System.Windows.Forms.Button Test_Tool_Runner;
        private System.Windows.Forms.Button Test_GetQRollupResults;
    }
}

