/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tema2;

/* Radoi Ioana Gabriela
 * Indexarea documentelor folosind paradigma Map-Reduce
 *
 *
 * Descriere rezolvare:
 *
 * Am folosit doua WorkPool-uri, in urmatorul fel :
 *
 *     -Am citit fisierele fragmentate in aproximativ argv[1] octeti cu ajutorul functiei read, astfel
 * fiecare fragment reprezentand o solutie partiala pe care am pus-o intr-un wookpool. Deci am format
 * task-uri pentru workeri de tip Mapper, care au in componenta sa un map de cuvinte, unde stochez toate
 * cuvintele din taskurile corespunzatoare workerului respectiv, impreuna cu numarul total de aparitii
 * ale acestora (Am 4 workeri de tip "Mapper" si taskurile corespunzatoare in WorkPool (fragmentele de text) ale
 * documentului la care sunt in acel moment. Fiecare Mapper isi ia un task (fragment din text) si ii numara
 * cuvintele, punand rezultatul in map-ul sau de cuvinte (al worker-ului respectiv), dupa care mai ia un task, ii numara
 * si acestuia cuvintele si aduna la cea avea deja de data trecuta si tot asa pana nu mai exista task-uri). Asta fac
 * fiecare dintre cei 4 "Mappei".
 *
 *      -Dupa ce s-au terminat task-urile, in main, preiau aceste map-uri de cuvinte
 * corespunzatoare Mapperi-lor. Apoi formez o solutie partiala pentru alt WorkPool, task-ul fiind format din
 * 4 map-uri de cuvinte pe care le-am preluat de la Mapperi. Si acelasi lucru se repeta pentru toate cele argv[3] documente.
 *
 *      -Dupa ce am terminat de pus taskuri in noul WorkPool, pornesc workeri de tip "putTogether", care iau solitii
 * partiale din noul woorkpool si imbina cele patru map-uri de cuvinte (asa cum am spus si mai sus, cele 4 map-uri de cuvinte
 * sunt corespunzatoare unuia dintre documentele citite). Dupa fiecare procesare, pun task-uri fomate
 * dintr-un map de cuvinte (rezultatul combinarii celor 4 map-uri de cuvinte) intr-un alt WorkPool (pentru simplificare,
 * acest nou WorkPool l-am luat ca fiind cel anterior, de unde si-au luat Mapperi fragmentele de text).
 *
 *      -Dupa ce workerii de tip putTogether au terminat, in main preiau noile taskuri din WorkPool si le dau workerilor
 * de tip Reduce, cu exeptia map-ului de cuvinte rezultat corespunzator primului document, pe care il voi da ca paramatru
 * acestor workeri de tip Reduce. Acestia au rolul de a calcula similitudinea intre map-ul de cuvinte primit ca parametru
 * si map-ul de cuvinte al task-ului preluat din WorkPool. Rezultatul il pune intr-un alt WorkPool si de acesta data
 * am preferat sa fie workpoolul care se golise anterior, cel unde si-au pus putTogetherii rezultatele. Apoi aceste
 * rezultate (taskuri) le preiau in main, dupa care le sortez si le afisez in fisierul de iesire. *
 */
import java.util.*;
import java.io.*;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.IOException;

//clasa PartialSolution -> o sa-mi definesc toate tipurile de solutii partiale de care am nevoie
class PartialSolution {

    int type;
    String line;
    int nr_doc;
    double similitudine;
    Map<String, Integer> cuvinte1;
    Map<String, Integer> cuvinte2;
    Map<String, Integer> cuvinte3;
    Map<String, Integer> cuvinte4;

    public PartialSolution(String s, int type) {
        this.type = type;
        this.line = s;
    }

    public PartialSolution(int nr_doc, Map<String, Integer> cuvinte1, Map<String, Integer> cuvinte2, Map<String, Integer> cuvinte3, Map<String, Integer> cuvinte4, int type) {
        this.nr_doc = nr_doc;
        this.cuvinte1 = cuvinte1;
        this.cuvinte2 = cuvinte2;
        this.cuvinte3 = cuvinte3;
        this.cuvinte4 = cuvinte4;
        this.type = type;
    }

    public PartialSolution(int nr_doc, Map<String, Integer> cuvinte1, int type) {
        this.nr_doc = nr_doc;
        this.cuvinte1 = cuvinte1;
        this.type = type;
    }

    public PartialSolution(int nr_doc, double similitudine, int type) {
        this.nr_doc = nr_doc;
        this.similitudine = similitudine;
        this.type = type;
    }

    public String toString() {
        if (this.type == 0) {
            return "partial text(pentru Maper): " + this.line;
        } else if (this.type == 1) {
            return "pentru Reduce (combinatii) : " + this.cuvinte1;
        } else if (this.type == 2) {
            return "colectile partiale pentru putTogether: " + "nr_doc: " + this.nr_doc + " primul: " + this.cuvinte1 + " al doilea: " + this.cuvinte2 + " al treilea: " + this.cuvinte3 + " al patrulea: " + this.cuvinte4;
        } else if (this.type == 3) {
            return "ce rezulta din putTogethe pt fiecare partial solution:" + " nr_doc: " + this.nr_doc + " mapul de cuvinte rezultat: " + this.cuvinte1;
        } else {
            return "similitudinea: " + " nr_doc: " + this.nr_doc + " sim: " + this.similitudine;
        }
    }
}

//clasa Mapper este pentru workerii de tip Mapper care iau task cu task (aceste taskuri reprezentand fragmente
//de text) si actualizeaza un map de cuvinte, unde pun cuvintele gasite in task si numarul de aparitii ale fiecaruia
class Mapper extends Thread {

    Map<String, Integer> cuvinte;  //mapul ce cuvinte
    WorkPool wp;   //workpool-ul de unde isi iau taskuri

    public Mapper(WorkPool workpool) {
        this.cuvinte = new TreeMap<String, Integer>();
        this.wp = workpool;
    }

    void processPartialSolution(PartialSolution ps) {

        String text_lower = ps.line.toLowerCase();  //am transformat tot fragmentul in litere mici
        //am delimitat fragmentul, astfel am preluat toate cuvintele din fragment
        StringTokenizer delimitat = new StringTokenizer(text_lower, " \t\n\r\f,.-+*~`' \" \\ !?; : ()  ");

        String cuvant = null;
        int gasit = 0;  //"gasit" este o variabila care ia valoarea 0 daca nu am gasit cuvantul in map-ul de cuvinte al
        //worker-ului si valoarea 1 daca l-am gasit

        while (delimitat.hasMoreTokens()) {   //parcurg toate cuvintele fragmentului
            cuvant = delimitat.nextToken();        //cuvantul la care am ajuns
            gasit = 0;

            Set<Map.Entry<String, Integer>> set = cuvinte.entrySet();   //setul de cuvinte in stadiul corespunzator
            //while-ului
            Iterator<Map.Entry<String, Integer>> iter;
            iter = set.iterator();
            while (iter.hasNext()) {  //parcurg cu iter cuvintele din setul "cuvinte"
                Map.Entry<String, Integer> map = iter.next();
                if (map.getKey().equals(cuvant)) {//daca am gasit vreun cuvant egal cu cel la care am ajuns
                    //in fragment, "gasit" ia valoarea 1
                    gasit = 1;
                    map.setValue(map.getValue() + 1);  //ii cresc valoarea cuvantului respectiv cu 1
                    break;
                }
            }
            if (gasit == 0) {   //daca acel cuvant din fragment nu exista in setul de cuvinte actual, il adaug in setul de
                //cuvinte actual
                cuvinte.put(cuvant, 1);
            }
        }
    }

    public void run() {
        System.out.println("Thread-ul worker " + this.getName() + " a pornit...");
        while (true) {
            PartialSolution ps = this.wp.getWork();
            if (ps == null) {
                break;
            }
            processPartialSolution(ps);
        }
        System.out.println("Thread-ul worker " + this.getName() + " s-a terminat...");
    }
}

//workerii de tip putTogether imbina cele 4 map-uri aferente taskului preluat din WorkPool
class putTogether extends Thread {

    WorkPool wp1;  //preiau din acest WorkPool taskuri
    WorkPool wp2;  //pun in acest WorkPool taskuri

    public putTogether(WorkPool w1, WorkPool w2) {

        this.wp1 = w1;
        this.wp2 = w2;

    }

    void processPartialSolution(PartialSolution ps) {

        Map<String, Integer> cuvinte_de_comparat;
        cuvinte_de_comparat = new TreeMap<String, Integer>();

        Map<String, Integer> cuvinte_cu_care_compari;
        cuvinte_cu_care_compari = new TreeMap<String, Integer>();

        //initializez map-ul "cuvinte_de_comparat" cu primul map, si voi lua apoi pe rand celelalte map-uri
        //actualizand acest map "cuvinte_de_comparat"
        cuvinte_de_comparat = ps.cuvinte1;
        int k = 0;
        while (k < 3) {
            if (k == 0) {
                cuvinte_cu_care_compari = ps.cuvinte2;  //"cuvinte_cu_care_compari" va lua prima data map-ul cuvinte2

            } else if (k == 1) {
                cuvinte_cu_care_compari = ps.cuvinte3; //apoi cuvinte3
            } else {
                cuvinte_cu_care_compari = ps.cuvinte4; //apoi cuvinte4
            }

            Set<Map.Entry<String, Integer>> set = cuvinte_de_comparat.entrySet();   //am pus intr-un set stadiul
            //in care se afla "cuvinte_de_comparat"

            Iterator<Map.Entry<String, Integer>> iter;
            iter = set.iterator();

            int gasit = 0;
            while (iter.hasNext()) {  //parcurg "cuvinte_de_comparat"
                Map.Entry<String, Integer> map = iter.next();

                //vom pune in set2, fiecare dintre mapurile "cuvinte_cu_care_compari" declarate mai sus, pe rand
                Set<Map.Entry<String, Integer>> set2 = cuvinte_cu_care_compari.entrySet();

                Iterator<Map.Entry<String, Integer>> iter2;
                iter2 = set2.iterator();

                while (iter2.hasNext()) { //parcurg "cuvinte_cu_care_compari"
                    Map.Entry<String, Integer> map2 = iter2.next();
                    if (map2.getKey().equals(map.getKey())) {//daca cuvantul din "cuvinte_de_comparat"
                        //se regaseste in "cuvinte_cu_care_compari", atunci actualizez cuvinte_cu_care_compar
                        //adunand la valoarea (numarul de aparitii) corespunzatoare cuvantului , valoarea corespunzatoare
                        //cuvantului din "cuvinte_de_comparat"

                        map2.setValue(map2.getValue() + map.getValue());
                        gasit = 1;
                    }
                }

                if (gasit == 0) {
                    cuvinte_cu_care_compari.put(map.getKey(), map.getValue());  //daca nu am gasit cuvantul,
                    //atunci il adaug, cu valoarea (numarul de aparitii) din "cuvinte_de_comparat"
                }

                gasit = 0;
            }

            k++;  //trec la pasul urmator

            cuvinte_de_comparat = cuvinte_cu_care_compari;  //am imbinat map-ul "cuvinte_de_comparat" cu " cu map-ul
            //"cuvinte_cu_care_compari"
        }

        //dupa ce am imbinat toate cele 4 map-uri ale unui task, pun rezultatul intr-un alt WorkPool
        PartialSolution paS;
        paS = new PartialSolution(ps.nr_doc, cuvinte_de_comparat, 3);

        this.wp2.putWork(paS);

    }

    public void run() {
        System.out.println("Thread-ul worker " + this.getName() + " a pornit...");
        while (true) {
            PartialSolution ps = this.wp1.getWork();
            if (ps == null) {
                break;
            }

            processPartialSolution(ps);
        }
        System.out.println("Thread-ul worker " + this.getName() + " s-a terminat...");

    }
}

//workerii de tip "Reducer" calculeaza similitudinea conform formulei din tema, intre map-ul de cuvinte primit ca parametru
//si map-ul pe care il iau din WorkPool
class Reducer extends Thread {

    Map<String, Integer> cuvinte;
    WorkPool wp1;
    WorkPool wp2;

    public Reducer(WorkPool wp1, Map<String, Integer> cuvinte, WorkPool wp2) {
        this.wp1 = wp1;
        this.cuvinte = cuvinte;
        this.wp2 = wp2;

    }

    static int count_word(Map<String, Integer> cuvinte) {  //calculeza numar totalul de cuvinte dintr-un map de cuvinte
        //ce contine cuvintele si numarul lor de aparitii

        int nr_cuvinte = 0;
        Set<Map.Entry<String, Integer>> set = cuvinte.entrySet();  //iau un set de "cuvinte", si parcurg fiecare
        //cuvant din "cuvinte"

        Iterator<Map.Entry<String, Integer>> iter;
        iter = set.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Integer> map = iter.next();
            nr_cuvinte += map.getValue();   //adun toate aparitile tuturor cuvintelor, si pun rezultatul intr-un int

        }
        return nr_cuvinte;  //returnez rezultatul
    }

    void processPartialSolution(PartialSolution ps) {

        int nr_cuvinte1 = 0;
        int nr_cuvinte2 = 0;

        nr_cuvinte1 = count_word(this.cuvinte);  //numarul de cuvinte ale map-ului de cuvinte primit ca parametru
        nr_cuvinte2 = count_word(ps.cuvinte1);   //numarul de cuvinte ale map-ului de cuvinte luat din WorkPool

        double sum = 0;
        double multiplication = 0;

        Set<Map.Entry<String, Integer>> set = this.cuvinte.entrySet(); //iau un set unde pun map-ul de cuvinte primit
        //ca parametru

        Iterator<Map.Entry<String, Integer>> iter;
        iter = set.iterator();

        while (iter.hasNext()) {  //parcurg map-ul de cuvinte primit ca parametru

            Map.Entry<String, Integer> map = iter.next();
            Set<Map.Entry<String, Integer>> set2 = ps.cuvinte1.entrySet();   //iau un set unde pun map-ul de cuvinte
            //luat din WorkPool

            Iterator<Map.Entry<String, Integer>> iter2;
            iter2 = set2.iterator();

            while (iter2.hasNext()) { //parcurg map-ul de cuvinte luat din WorkPool

                Map.Entry<String, Integer> map2 = iter2.next();

                if (map2.getKey().equals(map.getKey())) {  //daca un cuvant se regaseste in ambele map-uri, aplic formula
                    //din enuntul temei
                    multiplication = (((double) map2.getValue() / nr_cuvinte2) * 100) * (((double) map.getValue() / nr_cuvinte1) * 100);
                    sum += multiplication / 100.0;  //adun la sum rezultatul
                }
            }
        }

        //pun rezultatul final intr-un alt WorkPool
        PartialSolution paS;
        paS = new PartialSolution(ps.nr_doc, (double) sum, 4);
        this.wp2.putWork(paS);
    }

    public void run() {
        System.out.println("Thread-ul worker " + this.getName() + " a pornit...");
        while (true) {
            PartialSolution ps = this.wp1.getWork();
            if (ps == null) {
                break;
            }
            processPartialSolution(ps);
        }
        System.out.println("Thread-ul worker " + this.getName() + " s-a terminat...");
    }
}

public class ReplicatedWorkers {

    //citesc fisierul pe fragmente aproximativ egale, si fiecare fragment il trimit intr-un workpool
    public static void read_file(File file, PartialSolution paS, WorkPool wp, int dim_parte) {

        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader in = null;
        try {
            //prin clasa File am facut referire la un fisier "file" din sistem
            fis = new FileInputStream(file);  //pe baza instantei File creez un flux de intrare de
            //tip FileInputStream
            isr = new InputStreamReader(fis); //folsind instanta FileInputStream, am creat
            //un InputStreamReader
            in = new BufferedReader(isr);//si pe baza InputStreamReader -ului am creat un
            //BufferedReader ce defineste comportamente care pot fi folosite pentru citirea
            //din fisier
        } catch (FileNotFoundException e) {

            e.printStackTrace();
        }

        int lungime = (int) file.length();  //lungimea fisierului
        String partT;  //textul partial pe care il voi trimite in WorkPool

        //am luat doi vectori de char-uri, primul reprezentand textul "dim_parte" minus 1 (dim_parte va fi argv[1] ce
        //reprezinta dimensiunea in octeti minima a textului partial)

        char[] buff1 = new char[dim_parte - 1];        //prima data citim dim_parte minus 1 caractere
        char[] buff2 = new char[1];        //aici o sa pun ultimul caracter (din cele minim argv[1] caractere
        //pe care trebuie sa le citesc)

        int end = 0; //unde se termina textul in fisier
        String car;
        while (end < lungime - dim_parte) {  //atata timp cat nu am ajuns la ultimul fragment

            try {
                in.read(buff1, 0, dim_parte - 1);  //citesc tot subsirul in afara de ultimul caracter
            } catch (IOException e) {
                e.printStackTrace();
            }
            end += dim_parte - 1;  //actualizez end  (inaintez in fisier)

            partT = new String(buff1); //partT devine String

            try {
                in.read(buff2, 0, 1); //am citit ultimul caracter
            } catch (IOException e) {
                e.printStackTrace();
            }
            end++;  //incrementez end (inaintez in fisier)
            car = new String(buff2);  //pun ultimul caracter in car

            while (!(car.equals(" ") || car.equals("\n")) & (end < lungime)) {  //verific daca car este egal cu " " sau "\n"

                partT = partT.concat(car);  //daca nu este " " sau "\n" il concatenez la textul meu partial

                try {
                    in.read(buff2, 0, 1); //am mai citit un caracter (si o sa tot citesc pana dau de " " sau "\n")
                } catch (IOException e) {

                    e.printStackTrace();
                }
                end++; //(inaintez in fisier)
                car = new String(buff2);    //pun caracterul citit in car
            }

            //dupa ce am format textul partial, il pun in WorkPool
            paS = new PartialSolution(partT, 0);
            wp.putWork(paS);
        }

        //pentru ultimul fragment
        buff1 = new char[lungime - end];  //ultimul text partial va avea "lungime" minus "end" caractere
        try {
            in.read(buff1, 0, lungime - end);
        } catch (IOException e) {

            e.printStackTrace();
        }
        partT = new String(buff1);

        //pun ultimul fragmrnt in WorkPool
        paS = new PartialSolution(partT, 0);
        wp.putWork(paS);

    }

    public static void main(String args[]) throws InterruptedException, FileNotFoundException, IOException {


        //m-am folosit de doua WorkPooluri
        WorkPool wp1 = new WorkPool(4);
        WorkPool wp2 = new WorkPool(4);

        PartialSolution paS = null;
        int doc;
        int dim = Integer.parseInt(args[1]);  //dimensiunea fragmentului in octeti
        double prag = Double.parseDouble(args[2]); //pragul minuim de la care pornesc ca sa afisez similaritatea

        for (doc = 0; doc < Integer.parseInt(args[3]); doc++) {
            read_file(new File(args[doc + 4]), paS, wp1, dim);  //citesc fisierul si il impart
            //pe fragmenta aproximativ egale, si le trimit intr-un WorkPool

            //mapperii
            Mapper one = new Mapper(wp1);
            Mapper two = new Mapper(wp1);
            Mapper three = new Mapper(wp1);
            Mapper four = new Mapper(wp1);
            one.start();
            two.start();
            three.start();
            four.start();
            one.join();
            two.join();
            three.join();
            four.join();

            PartialSolution four_maps;
            //pentru fiecare document, trimit taskuri in celalalt WorkPool
            four_maps = new PartialSolution(doc, one.cuvinte, two.cuvinte, three.cuvinte, four.cuvinte, 2);
            wp2.putWork(four_maps);
        }

        //dupa ce am citi toate documentele si am aflat map-urile partiale, startez putTogetherii,
        //care iau taskurile din noul WorkPool creat si imbina map-urile int-unul singur, punand
        //rezultatele intr-un alt WorkPool

        putTogether one_t = new putTogether(wp2, wp1);
        putTogether two_t = new putTogether(wp2, wp1);
        putTogether three_t = new putTogether(wp2, wp1);
        putTogether four_t = new putTogether(wp2, wp1);

        one_t.start();
        two_t.start();
        three_t.start();
        four_t.start();

        one_t.join();
        two_t.join();
        three_t.join();
        four_t.join();

        //preiau taskurile din WorkPool-ul creat (cu rezultatele putTogetherilor) si aflu care
        //este primul document (cel pe care il compar cu celelalte), iar pe celelalte 3 taskuri le pun in
        //celalalt WorkPool

        PartialSolution reducer;
        Map<String, Integer> first_doc;
        first_doc = new TreeMap<String, Integer>();

        for (int i = 0; i < Integer.parseInt(args[3]); i++) {
            PartialSolution ps = wp1.getWork();
            if (ps.nr_doc == 0) { //daca este primul
                first_doc = ps.cuvinte1;
            } else {
                reducer = new PartialSolution(ps.nr_doc, ps.cuvinte1, 1);
                wp2.putWork(reducer);
            }
        }

        //reducerii, ce au ca unul din  parametrii documentul pe care il compar cu celelalte
        //pun noile rezultate in wp1
        Reducer one_r = new Reducer(wp2, first_doc, wp1);
        Reducer two_r = new Reducer(wp2, first_doc, wp1);
        Reducer three_r = new Reducer(wp2, first_doc, wp1);
        Reducer four_r = new Reducer(wp2, first_doc, wp1);

        one_r.start();
        two_r.start();
        three_r.start();
        four_r.start();

        one_r.join();
        two_r.join();
        three_r.join();
        four_r.join();

        //preiau noile taskuri din wp1 in doi vectorii
        double[] similitudine = new double[Integer.parseInt(args[3])];  //similitudinea
        int[] nr_document = new int[Integer.parseInt(args[3])];  //numarul documentului

        for (int i = 0; i < Integer.parseInt(args[3]) - 1; i++) {
            PartialSolution ps = wp1.getWork();
            similitudine[i] = ps.similitudine;
            nr_document[i] = ps.nr_doc;
        }

        //sortez vectorul similitudine
        double aux = 0;
        int aux2;
        for (int i = 0; i < Integer.parseInt(args[3]) - 1; i++) {
            for (int j = 0; j < Integer.parseInt(args[3]) - 1; j++) {

                if (similitudine[i] > similitudine[j]) {
                    aux = similitudine[i];
                    aux2 = nr_document[i];

                    similitudine[i] = similitudine[j];
                    nr_document[i] = nr_document[j];
                    similitudine[j] = aux;
                    nr_document[j] = aux2;
                }
            }
        }

        // afisez datele in fisierul de iesire cu ajutorul clasei PrintReader
        File file = new File("output.txt");
        try (PrintStream out = new PrintStream(file)) {
            out.println("Rezultate pentru: (" + args[0] + ")");
            out.println();
            for (int i = 0; i < Integer.parseInt(args[3]) - 1; i++) {
                if (similitudine[i] > prag) {
                    //trunchez la 3 zecimale
                    int d = (int) (similitudine[i] * 1000);
                    double d2 = (double) d / 1000;
                    out.println(args[4 + nr_document[i]] + " (" + d2 + "%)");
                }
            }
        }
    }
}
