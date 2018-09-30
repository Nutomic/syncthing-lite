# Add project specific ProGuard rules here.
# By default, the flags in this file are appended to flags specified
# in /home/jonas/android-studio/sdk/tools/proguard/proguard-android.txt
# You can edit the include path and order by changing the proguardFiles
# directive in build.gradle.
#
# For more details, see
#   http://developer.android.com/guide/developing/tools/proguard.html

# Add any project specific keep options here:

# If your project uses WebView with JS, uncomment the following
# and specify the fully qualified class name to the JavaScript interface
# class:
#-keepclassmembers class fqcn.of.javascript.interface.for.webview {
#   public *;
#}

# ensure that stack traces make sense
-keepattributes SourceFile,LineNumberTable

# ensure that the config can be read
-keep class net.syncthing.java.core.configuration.** { *; }
-keep class net.syncthing.java.core.beans.DeviceInfo { *; }
-keep class net.syncthing.java.core.beans.FolderInfo { *; }
-keep class net.syncthing.java.core.beans.DeviceId { *; }

-keepattributes Signature
-keepattributes *Annotation*
-keep class sun.misc.Unsafe { *; }

# this library uses factories with reflection
-keep class net.jpountz.lz4.** { *; }

# disable warnings
-dontwarn com.google.protobuf.UnsafeUtil
-dontwarn com.google.protobuf.UnsafeUtil$1
-dontwarn net.jpountz.util.UnsafeUtils
-dontwarn org.bouncycastle.cert.dane.fetcher.JndiDANEFetcherFactory
-dontwarn org.bouncycastle.cert.dane.fetcher.JndiDANEFetcherFactory$1
-dontwarn org.bouncycastle.jce.provider.X509LDAPCertStoreSpi
-dontwarn org.bouncycastle.mail.smime.CMSProcessableBodyPart
-dontwarn org.bouncycastle.mail.smime.CMSProcessableBodyPartInbound
-dontwarn org.bouncycastle.mail.smime.CMSProcessableBodyPartOutbound
-dontwarn org.bouncycastle.mail.smime.examples.CreateCompressedMail
-dontwarn org.bouncycastle.mail.smime.examples.CreateEncryptedMail
-dontwarn org.bouncycastle.mail.smime.examples.CreateLargeCompressedMail
-dontwarn org.bouncycastle.mail.smime.examples.CreateLargeEncryptedMail
-dontwarn org.bouncycastle.mail.smime.examples.CreateLargeSignedMail
-dontwarn org.bouncycastle.mail.smime.examples.CreateSignedMail
-dontwarn org.bouncycastle.mail.smime.examples.CreateSignedMultipartMail
-dontwarn org.bouncycastle.mail.smime.examples.ExampleUtils
-dontwarn org.bouncycastle.mail.smime.examples.ReadCompressedMail
-dontwarn org.bouncycastle.mail.smime.examples.ReadEncryptedMail
-dontwarn org.bouncycastle.mail.smime.examples.ReadLargeCompressedMail
-dontwarn org.bouncycastle.mail.smime.examples.ReadLargeEncryptedMail
-dontwarn org.bouncycastle.mail.smime.examples.ReadLargeSignedMail
-dontwarn org.bouncycastle.mail.smime.examples.ReadSignedMail
-dontwarn org.bouncycastle.mail.smime.examples.SendSignedAndEncryptedMail
-dontwarn org.bouncycastle.mail.smime.examples.ValidateSignedMail
-dontwarn org.bouncycastle.mail.smime.handlers.multipart_signed
-dontwarn org.bouncycastle.mail.smime.handlers.multipart_signed$LineOutputStream
-dontwarn org.bouncycastle.mail.smime.handlers.PKCS7ContentHandler
-dontwarn org.bouncycastle.mail.smime.handlers.pkcs7_mime
-dontwarn org.bouncycastle.mail.smime.handlers.pkcs7_signature
-dontwarn org.bouncycastle.mail.smime.handlers.x_pkcs7_mime
-dontwarn org.bouncycastle.mail.smime.handlers.x_pkcs7_signature
-dontwarn org.bouncycastle.mail.smime.SMIMECompressed
-dontwarn org.bouncycastle.mail.smime.SMIMECompressedGenerator
-dontwarn org.bouncycastle.mail.smime.SMIMECompressedGenerator$1
-dontwarn org.bouncycastle.mail.smime.SMIMECompressedGenerator$ContentCompressor
-dontwarn org.bouncycastle.mail.smime.SMIMECompressedParser
-dontwarn org.bouncycastle.mail.smime.SMIMEEnveloped
-dontwarn org.bouncycastle.mail.smime.SMIMEEnvelopedGenerator
-dontwarn org.bouncycastle.mail.smime.SMIMEEnvelopedGenerator$1
-dontwarn org.bouncycastle.mail.smime.SMIMEEnvelopedGenerator$ContentEncryptor
-dontwarn org.bouncycastle.mail.smime.SMIMEEnvelopedParser
-dontwarn org.bouncycastle.mail.smime.SMIMEGenerator
-dontwarn org.bouncycastle.mail.smime.SMIMESigned
-dontwarn org.bouncycastle.mail.smime.SMIMESigned$1
-dontwarn org.bouncycastle.mail.smime.SMIMESignedGenerator
-dontwarn org.bouncycastle.mail.smime.SMIMESignedGenerator$1
-dontwarn org.bouncycastle.mail.smime.SMIMESignedGenerator$ContentSigner
-dontwarn org.bouncycastle.mail.smime.SMIMESignedParser
-dontwarn org.bouncycastle.mail.smime.SMIMESignedParser$1
-dontwarn org.bouncycastle.mail.smime.SMIMEToolkit
-dontwarn org.bouncycastle.mail.smime.SMIMEUtil
-dontwarn org.bouncycastle.mail.smime.SMIMEUtil$LineOutputStream
-dontwarn org.bouncycastle.mail.smime.SMIMEUtil$WriteOnceFileBackedMimeBodyPart
-dontwarn org.bouncycastle.mail.smime.util.FileBackedMimeBodyPart
-dontwarn org.bouncycastle.mail.smime.util.SharedFileInputStream
-dontwarn org.bouncycastle.mail.smime.validator.SignedMailValidator
-dontwarn org.bouncycastle.x509.util.LDAPStoreHelper
