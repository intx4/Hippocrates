for i in (seq 0 100); 
    echo $i >> output.txt; 
    ./test --totalNode $i --repeats 100 >> output.txt; 
    end
