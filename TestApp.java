import gr.phaistosnetworks.tank.ByteManipulator;
import gr.phaistosnetworks.tank.TankClient;
import gr.phaistosnetworks.tank.TankException;
import gr.phaistosnetworks.tank.TankMessage;
import gr.phaistosnetworks.tank.TankRequest;
import gr.phaistosnetworks.tank.TankResponse;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

class TestApp {
  public static void main(String[] args) throws Exception {
    byte [] foo;
    String [] testStrings = new String[4];
    testStrings[0] = "Hello World";
    testStrings[1] = "Here is the super long string that should make my str8 implementation explode. How many chars can 1 byte enumerate anyway? How about a spider bite. That has 8 legs and maybe 8 eyes. It may have spider sense too. yada yada bladi blah Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch";
    testStrings[2] = "";
    testStrings[3] = "myKey";

    for (String testString : testStrings) {
      try {
        foo = ByteManipulator.getStr8(testString);
        String myStr8 = new String(new ByteManipulator(ByteBuffer.wrap(foo)).getStr8().array());
        if (! myStr8.equals(testString)) {
          System.err.println("Str8 conversion is broken");
          System.err.println("Expected:" + testString + "\n but got:" + myStr8);
          System.exit(1);
        }
      } catch (TankException te) {
        System.err.println(te.getMessage());
      }
    }

    long[] testVals = new long[9];
    testVals[0] = 5L;
    testVals[1] = 180L;
    testVals[2] = 307L;
    testVals[3] = 512L;
    testVals[4] = 1790L;
    testVals[5] = 23456L;
    testVals[6] = 9990004L;
    testVals[7] = 1470905444L;
    testVals[8] = 1470905444156L;

    long testOutput;
    //Test long
    for (long testVal : testVals) {
      foo = ByteManipulator.serialize(testVal, 64);

      testOutput = new ByteManipulator(ByteBuffer.wrap(foo)).deSerialize(64);
      if (testOutput != testVal) {
        System.err.println("deserialization is broken");
        System.err.println("Expected:" + testVal + " but got:" + Long.toString(testOutput));
        System.exit(1);
      }
    }

    //Test varInt
    for (long testVal : testVals) {
      try {
        foo = ByteManipulator.getVarInt(testVal);
        testOutput = new ByteManipulator(ByteBuffer.wrap(foo)).getVarInt();
        if (testOutput != testVal) {
          System.err.println("Varint conversion is broken");
          System.err.println("Expected:" + testVal + "\n but got:" + testOutput);
          System.exit(1);
        }
      } catch (TankException te) {
        System.err.println(te.getMessage());
      }

    }

    TankClient tc = new TankClient("localhost", 11011);
    List<TankResponse> responses;

    TankRequest publish = new TankRequest(TankClient.PUBLISH_REQ);
    publish.publishMessage("foo", 0, new TankMessage(
        ByteBuffer.wrap("akey".getBytes()),
        ByteBuffer.wrap("Some Random Text".getBytes())));
    publish.publishMessage("foo", 0, new TankMessage(
        ByteBuffer.wrap("anotherkey".getBytes()),
        ByteBuffer.wrap("Some other Random Text".getBytes())));
    publish.publishMessage("foo", 0, new TankMessage(ByteBuffer.wrap("Who needs keys anyway?".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap("here, have some partition magic".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(
        ByteBuffer.wrap("akey".getBytes()),
        ByteBuffer.wrap("more partition magic".getBytes())));
    publish.publishMessage("foo", 2, new TankMessage(ByteBuffer.wrap("This one should fail".getBytes())));
    publish.publishMessage("bar", 0, new TankMessage(ByteBuffer.wrap("hello world".getBytes())));
    publish.publishMessage("randomness", 0, new TankMessage(ByteBuffer.wrap("This should fail too".getBytes())));
    publish.publishMessage("randomness", 1, new TankMessage(ByteBuffer.wrap("aaand this".getBytes())));
    publish.publishMessage("foo", 0, new TankMessage(ByteBuffer.wrap(
        "Συγγένειας παρωδώντας του όλα πλασματική συνάντησης ξεχειλίζει και. Μη καλά άλλη αργά όψιν τη ούτε σο στις να. Επίπεδο σημασία να κάποιον απ ως τα έχοντας. Συνειρμικά διαχρονικά παραδεχτεί καθ νου. Καθηγητής φθείρεται κοινωνικά τα το παραλλαγή. Στα διάφορες αργότερα μην εξαιτίας βρεγμένο επιλέξει όσο υπάρχουν. Κόψει μόνος ατ σοφού τι φωνών έχουν. Νου εκουσίως σύνδρομο επιμονής προσώπου τον. Πρόσμιξη στο όλη μερακλής οργάνωση δεύτερον.  Γεφυρώνει καταγωγής βαλαβάνης τα μαθητείας σε δε. Παράδοση γνωρίζει πως ολόκληρο ελληνική πιο ουδέτερη ένα άνθρωπος. Αποβαίνει σύγχρονου στηρίξουν σκαρωμένη απ νε. Ενώ κέντρο σημείο rites έμμεση απ΄ ψυχικά και μικρές. Λογαριασμό διαμόρφωσε πατημασιές ως αποτέλεσμά σο υποσχέθηκε να σε. Ρου ζώγ μυθοπλασία κυριότερες τελευταίες και. Πιο φίλ άλλες γνώση πώς χάσει χώρου ούτως ήρωας.  Τι πω να εμπειρίες ως συντελούν αποδίδουν ιδιαίτερα ιστορικές. Φουσκωμένα στρατηγική εκ αποτέλεσμα γεωγραφική νε. Σε θα αναζήτησης προέρχεται παραμέρισε αν εκ ειδυλλιακή. Ακριβώς απόδοση τα χαμένης νε γίνεται έχοντας ως τη. Ώρα υπόλοιπη των αφεντικό κοινωνία αρνητικά. Μερακλής θεωρηθεί όλα προφανώς ζωή συνταγές την αντίληψη καθαυτής. Μην παράδειγμα φινλανδική ανθρωπίνων διαχρονική από ενώ διαβήματος. Αμφότερες στη προλόγους σύγχρονοι κλπ ατο σύγχρονες.  Τι με υλικού λαϊκών μη ως χείλος λογική θυμικά πείρας. Αδειάσει στο μην ποντιακή μητρικής μηνύματα περίοδος εργασίας. Απ πιει άλλη τη ίδια τσεν κακό τα ωχρό. Μητέρας απ απαγωγή πυρήνας παιδιών αν μάλιστα αρ να. Κάτι πόνο νέας αρχή όχι στα ίδια δίνω. Ένα προέρχεται στερεότυπη χουμαρτζής ισοδυναμεί ζέη. Βλέπετε παιδικά εγγενώς τα έκλαιεν υπ. Είτε τι θέση γιου νέοι ως κάτι τα ατ. Κοινωνία αργότερα κουλιζάρ εφ τι με. Σχισμής αν εφ τι αρ περούκα επώνυμη λάζαρος. Στα ανάσκελα του σαν ωμότητες δύο παιδικής. Κι μπροστινής κατακτήσει παραπέμπει ρεαλιστικό σοβαρότητα πα νε. Συνιστώσες διαβάζουμε χρειάζεται το να ανέκφραστη ως εθελοντική καθίσταται. Παλιούς αληθινή που νυχτικά της.  Πα τύπου εκ τι πλέον πλοκή αθήνα. Γνωστικής κεί εκάθουζεν πει την προκύπτει ανεύρεσις διατήρηση. Μιλά ντεν ρου στο αργά κιτς δέβα. Τυχόν κενές ζωή εαυτό γραφή μου. Την καθ προσωπείων παραλλαγές λειτουργία επινοείται διά εάν. Συμβάσεις αποδείξει έως παπούτσια όχι σιδερένιο λιποθυμία των. Συν σου ενώ κεί καθίσταται καταθέσουν μεσημεριού αγαπημένου.  Αρπάζει έλληνες δε επιλογή τη να βαγγέλη. Πόνος συχνά να χρόνο έν. Αναφορικά νεότερους νε οι συστατικά σταμάτημα. Έστελνε να εμβήκεν επώνυμη μη. Πώς αφετέρου εξαιτίας μοναδικό δεν πρόλογος. Τυλισίμ τη γι ανήκουν γεγονός να. Ηθικές μερικά στη επίσης σύμπαν ιδρώτα αγάπης των εδώ κεί.  Μερ την συνεπώς ένα όλη βιβλίου σπιτιού. Νεότερη μην του διά σην άκαμπτη κρεβάτι εστίαση οπτικές ακροατή. Συγγένειας σε αποκάλυπτε συνδέοντας οπωσδήποτε τα έν διαλύοντας δημοσίευση αρ. Ως δειλία τα ένιωθε δεξιού. Επικρατούν δύο ρου κοινωνικές από αντάλλαγμα ροζ ιδιομορφία σταματήσει διηγημάτων. Του πλοκή οποίο κλπ initiation. Τα κατοχή αρ δηλαδή ντιβάν τα λιώσει κόσμοι κίνησή τη. Ακουσε χωρική πέρασε τα τι ας βάσανα μάλλον αρ.  Άρα υπό ενήλικο ατο κλπ διηνεκή μάλιστα χαμένης. Λόγια ναι δομές μια τυχόν θεσμό για. Μεγάλα θέληση αρ τα λογικό απαλές κι δε σήμερα βάσανα. Rites της ατο ομοιότητες μορφολογία κλπ ανθρωπίνων. Επιστολές φοβετζέας λιποθυμία εφ ατ σχολιάζει. Ρεαλισμός κι θεωρητική ιδιαίτερα τα το οδοιπορία αγαπημένο να. Παιχνίδια στη όλη εχτύπεσεν παραμύθια αναγνώσει γνωρίζουν.  Όχι γεγονότα όλο εξαιτίας αγγελίες πρακτική αντλούσε των. Παλαιότερα ρου ενώ χουμαρτζής προσελκύει μην σου. Διά των ποιο μέσω γαρή νέου και μένη υγρή. Εάν παράγει εφηβεία σαν διαφορά καθ. Όλα συγχρονική περιπέτεια προσποίηση τις ρου. Πιο εξω λόγω πόνο έτσι πρωί ζώγ. Οι γι καθαυτής κυρλοβίτ εφ εναντίον εκουσίως βιζυηνού ανάγνωση θα. Ανθρώπους τι θα κλιμάκωση αποδέκτες παράρτημα καθηγητής μη. Ιδιαίτερης σημαντικές γι το σπανιότατη. Αν οι τι δύναμη έπρεπε στίχοι".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 1".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 2".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 3".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 4".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 5".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 6".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 7".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 8".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 9".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 1".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 2".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 3".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 4".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 5".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 6".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 7".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 8".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 9".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 1".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 2".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 3".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 4".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 5".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 6".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 7".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 8".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 9".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 1".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 2".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 3".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 4".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 5".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 6".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 7".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 8".getBytes())));
    publish.publishMessage("foo", 1, new TankMessage(ByteBuffer.wrap(
        "Μία μακρουλή γραμμή για να περάσουμε το όριο των 1024 μπάιτς στο πιτς φιτιλι 9".getBytes())));

    if (false) {
      responses = tc.publish(publish);
      for (TankResponse tr : responses) {
        if (tr.hasError()) {
          System.out.format("Error: %d \n", tr.getError());
          if (tr.getError() == TankClient.ERROR_NO_SUCH_TOPIC) {
            System.out.println(
                "Error, topic " + tr.getTopic() + " does not exist !");
          } else if (tr.getError() == TankClient.ERROR_NO_SUCH_PARTITION) {
            System.out.println(
                "Error, topic " + tr.getTopic()
                + " does not have a partition " + tr.getPartition());
          } else if (tr.getError() == TankClient.ERROR_INVALID_SEQNUM) {
            System.out.println(
                "Error, topic " + tr.getTopic()
                + " : " + tr.getPartition() + " Invalid Sequence Num");
          } else {
            System.out.println(
                "System error for topic: " + tr.getTopic() + " partition: " + tr.getPartition());
          }
        }
      }
    }


    if (true) {
      TankRequest consume = new TankRequest(TankClient.CONSUME_REQ);
      consume.consumeTopicPartition("foo", 0, 0, fetchSize);
      consume.consumeTopicPartition("foo", 1, 0, fetchSize);
      consume.consumeTopicPartition("bar", 0, 0, fetchSize);
      /*
      consume.consumeTopicPartition("randomness", 0, 0, fetchSize);
      consume.consumeTopicPartition("foo", 0, 99999, fetchSize);
      */

      long nextSeqNum = 0L;
      while (true) {
        responses = tc.consume(consume);
        consume = new TankRequest(TankClient.CONSUME_REQ);
        for (TankResponse tr : responses) {
          System.out.println("topic: " + tr.getTopic()
              + " partition: " + tr.getPartition()
              + " nextSeqNum: " + tr.getNextSeqNum()
              + " firstAvailSeqNum: " + tr.getFirstAvailSeqNum()
              + " highWaterMark: " + tr.getHighWaterMark());

          if (tr.getFetchSize() > fetchSize) {
            fetchSize = tr.getFetchSize();
          }

          if (tr.hasError() && tr.getError() == TankClient.ERROR_OUT_OF_BOUNDS) {
            if (tr.getRequestSeqNum() < tr.getFirstAvailSeqNum()) {
              nextSeqNum = tr.getFirstAvailSeqNum();
            } else if (tr.getRequestSeqNum() > tr.getHighWaterMark()) {
              nextSeqNum = tr.getHighWaterMark();
            }
          } else {
            nextSeqNum = tr.getNextSeqNum();
          }

          for (TankMessage tm : tr.getMessages()) {
            System.out.println("seq: " + tm.getSeqNum()
                + " ts: " + tm.getTimestamp()
                + " key: " + tm.getKey().array().toString()
                + " message: " + tm.getMessage().array().toString());
          }

          consume.consumeTopicPartition(
              tr.getTopic(),
              tr.getPartition(),
              nextSeqNum,
              fetchSize);
          System.out.println(
              "Next: " + tr.getTopic()
              + ":" + tr.getPartition()
              + " @" + nextSeqNum
              + " #" + tr.getFetchSize());
        }
        Thread.sleep(1000);
      }
    }
  }

  private static long fetchSize = 20000L;
}
