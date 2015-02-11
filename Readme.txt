Radoi Ioana Gabriela
Indexarea documentelor folosind paradigma Map-Reduce


Am folosit doua WorkPool-uri, in urmatorul fel :

		 -Am citit fisierele fragmentate in aproximativ argv[1] octeti cu ajutorul functiei read, astfel fiecare fragment reprezentand o solutie partiala pe care am pus-o intr-un wookpool. Deci am format task-uri pentru workeri de tip Mapper, care au in componenta sa un map de cuvinte, unde stochez toate
cuvintele din taskurile corespunzatoare workerului respectiv, impreuna cu numarul total de aparitii ale acestora (Am 4 workeri de tip "Mapper" si taskurile corespunzatoare in WorkPool (fragmentele de text) ale documentului la care sunt in acel moment. Fiecare Mapper isi ia un task (fragment din text) si ii numara cuvintele, punand rezultatul in map-ul sau de cuvinte (al worker-ului respectiv), dupa care mai ia un task, ii numara si acestuia cuvintele si aduna la cea avea deja de data trecuta si tot asa pana nu mai exista task-uri). Asta fac fiecare dintre cei 4 "Mappei".

		 -Dupa ce s-au terminat task-urile, in main, preiau aceste map-uri de cuvinte corespunzatoare Mapperi-lor. Apoi formez o solutie partiala pentru alt WorkPool, task-ul fiind format din 4 map-uri de cuvinte pe care le-am preluat de la   Mapperi. Si acelasi lucru se repeta pentru toate cele argv[3] documente.
	 
		 -Dupa ce am terminat de pus taskuri in noul WorkPool, pornesc workeri de tip "putTogether", care iau solitii partiale din noul woorkpool si imbina cele patru map-uri de cuvinte (asa cum am spus si mai sus, cele 4 map-uri de cuvinte sunt corespunzatoare unuia dintre documentele citite). Dupa fiecare procesare, pun task-uri fomate dintr-un map de cuvinte (rezultatul combinarii celor 4 map-uri de cuvinte) intr-un alt WorkPool (pentru simplificare, acest nou WorkPool l-am luat ca fiind cel anterior, de unde si-au luat Mapperi fragmentele de text).

		 -Dupa ce workerii de tip putTogether au terminat, in main preiau noile taskuri din WorkPool si le dau workerilor de tip Reduce, cu exeptia map-ului de cuvinte rezultat corespunzator primului document, pe care il voi da ca paramatru acestor workeri de tip Reduce. Reduce-eri au rolul de a calcula similitudinea intre map-ul de cuvinte primit ca parametru si map-ul de cuvinte al task-ului preluat din WorkPool. Rezultatul il vor pune intr-un alt WorkPool si de acesta data am preferat sa fie workpoolul care se golise anterior, cel unde si-au pus putTogetherii rezultatele. Apoi aceste rezultate (taskuri) le preiau in main, dupa care le sortez si le afisez in fisierul de iesire.
