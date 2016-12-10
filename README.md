# Wikipedia-Page-Rank
Wikipedia Page Rank using Google page rank algorithm

One of the biggest changes in our lives in the decade was the availability of efficient and
accurate web search. Google is the first search engine which is able to defeat the spammers
who had made search almost useless. The technology innovation behind Google is called
PageRank. This project is to implement the PageRank to find the most important Wikipedia
pages on the provided the adjacency graph extracted from Wikipedia dataset using AWS Elastic
MapReduce(EMR).

1.1 Definition of PageRank

PageRank is a function that assigns a real number to each page in the Web. The intent is that
the higher the PageRank of a page, the more important it is.  In our
project, we do not use teleport to deal with the sink node for simplicity. At the initial point,
each page is initialized with PageRank 1/N and the sum of the PageRank is 1. But the sum will
gradually decrease with the iterations due to the PageRank leaking in the sink nodes.
