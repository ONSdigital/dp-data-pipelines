```mermaid
flowchart TD
    subgraph "Development"
        dev[Developer Creates Feature]
        pr[Create PR to merge feature into sandbox]
        rel[Create release/* branch targeting sandbox]
    end
    
    dev --> pr
    pr --> rel
    
    subgraph "Sandbox Pipeline"
        increment["1 Increment Python version with -rcX suffix"]
        tag["2 Create and tag commit (v1.2.3-rc1)"]
        build["3 Build code"]
        publish["4 Publish Git release with artifacts"]
    end
    
    rel --> increment
    increment --> tag
    tag --> build
    build --> publish
    
    subgraph "Staging Pipeline"
        promote_staging["Promote to staging"]
        remove_suffix["Remove -rcX suffix (v1.2.3)"]
        tag_staging["Create new tag without rebuilding"]
        publish_staging["Publish Git release using same artifacts"]
    end
    
    publish --> promote_staging
    promote_staging --> remove_suffix
    remove_suffix --> tag_staging
    tag_staging --> publish_staging
    
    subgraph "Production Pipeline"
        promote_prod["Promote to production"]
        validation["Validation checks"]
        approval["Manual approval"]
        deploy_prod["Deploy to production using same artifacts"]
        verify["Post-deployment verification"]
    end
    
    publish_staging --> promote_prod
    promote_prod --> validation
    validation --> approval
    approval --> deploy_prod
    deploy_prod --> verify
```

```mermaid
%%{init: { 'logLevel': 'debug', 'theme': 'base', 'gitGraph': {'showBranches': true, 'showCommitLabel':true,'mainBranchName': 'sandbox'}} }%%
gitGraph
    commit
    branch staging
    commit id: "Staging branch created"
    branch production
    commit id: "Production branch created"
    checkout sandbox
    branch feature-1
    checkout feature-1
    commit
    commit
    checkout sandbox
    merge feature-1
    branch "release/1.2.3"
    checkout "release/1.2.3"
    commit id: "prepare release"
    checkout sandbox
    merge "release/1.2.3" tag: "v1.2.3-rc1"
```
