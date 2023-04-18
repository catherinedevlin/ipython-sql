// https://docsearch.algolia.com/docs/DocSearch-v3#implementation
// 
// Since we can't add a custom element to article-header, we wait until
// DOM is ready and creating a new element - #docsearch
// After the element was added to the DOM, we initialize docsearch. 

addEventListener("DOMContentLoaded", (event) => {
  const container = document.querySelector(".article-header-buttons");
  let docsearchDiv = document.createElement("DIV")
  docsearchDiv.id = 'docsearch';
  container.appendChild(docsearchDiv);

  setTimeout(() => {
    docsearch({
      container: '#docsearch',
      appId: 'Y6L7HQ2HZO',
      indexName: 'ploomber_jupysql',
      apiKey: '9a1fd3379e6d318ef4f46aa36a3c5fe6'
    });
  }, 100);

});


