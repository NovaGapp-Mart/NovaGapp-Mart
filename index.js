const functions = require("firebase-functions");
const admin = require("firebase-admin");
const nodemailer = require("nodemailer");

admin.initializeApp();

/* EMAIL CONFIG (GMAIL) */
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: "YOUR_GMAIL@gmail.com",
    pass: "YOUR_GMAIL_APP_PASSWORD"
  }
});

/* AUTO EMAIL ON NEW SUPPORT MESSAGE */
exports.sendSupportEmail = functions.firestore
  .document("support/{docId}")
  .onCreate(async (snap, context) => {

    const d = snap.data();

    const mailOptions = {
      from: "Fluence Support <YOUR_GMAIL@gmail.com>",
      to: "prashikbhalerao0208@leopro.tech",
      subject: "📩 New Help / Feedback Received",
      html: `
        <h2>New Support Message</h2>
        <p><b>User UID:</b> ${d.uid}</p>
        <p><b>User Email:</b> ${d.email}</p>
        <p><b>Message:</b></p>
        <p>${d.message}</p>
        <hr>
        <small>Status: ${d.status}</small>
      `
    };

    await transporter.sendMail(mailOptions);
    return null;
  });